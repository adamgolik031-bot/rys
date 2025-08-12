use fantoccini::elements::Element;
use fantoccini::{ClientBuilder, Locator};
use num_cpus;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio;
use tokio::sync::{Mutex, Semaphore};
use warp::Filter;

// Struktura do przechowywania danych scraping'u
#[derive(Clone)]
struct ScrapingState {
    data: Arc<Mutex<Vec<Value>>>,
    info: Arc<Mutex<Value>>,
}

async fn create_browser_client() -> Result<fantoccini::Client, fantoccini::error::CmdError> {
    let mut caps = serde_json::map::Map::new();
    let chrome_opts = serde_json::json!({
        "args": [
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-blink-features=AutomationControlled",
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "--window-size=1280,720",
            "--disable-extensions",
            "--disable-plugins",
            "--disable-web-security",
            "--disable-logging",
            "--disable-background-timer-throttling",
            "--disable-backgrounding-occluded-windows",
            "--disable-renderer-backgrounding",
            "--disable-features=TranslateUI",
            "--disable-ipc-flooding-protection",
            "--no-first-run",
            "--no-default-browser-check",
            "--memory-pressure-off",
            "--max_old_space_size=4096",
            "--aggressive-cache-discard",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync"
        ]
    });
    caps.insert("goog:chromeOptions".to_string(), chrome_opts);

    let client = ClientBuilder::native()
        .capabilities(caps)
        .connect("http://localhost:4444")
        .await
        .expect("Failed to connect to WebDriver");

    client.set_window_rect(0, 0, 1280, 720).await?;
    Ok(client)
}

async fn extract_thumbnail_and_title(
    client: &fantoccini::Client,
) -> (Option<String>, Option<String>) {
    // Extract thumbnail
    let thumbnail = {
        let selectors = vec!["div.thumbz a img"];

        let mut thumb_url = None;
        for selector in selectors {
            if let Ok(elements) = client.find_all(Locator::Css(selector)).await {
                for element in elements {
                    if selector.contains("meta") {
                        if let Ok(Some(content)) = element.attr("content").await {
                            if !content.is_empty()
                                && (content.starts_with("http") || content.starts_with("//"))
                            {
                                thumb_url = Some(if content.starts_with("//") {
                                    format!("https:{}", content)
                                } else {
                                    content
                                });
                                break;
                            }
                        }
                    } else {
                        // Fix: Handle async calls properly
                        let src = if let Ok(Some(src)) = element.attr("src").await {
                            Some(src)
                        } else if let Ok(Some(data_src)) = element.attr("data-src").await {
                            Some(data_src)
                        } else {
                            None
                        };

                        if let Some(src) = src {
                            if !src.is_empty() && (src.starts_with("http") || src.starts_with("//"))
                            {
                                thumb_url = Some(if src.starts_with("//") {
                                    format!("https:{}", src)
                                } else {
                                    src
                                });
                                break;
                            }
                        }
                    }
                }
                if thumb_url.is_some() {
                    break;
                }
            }
        }
        thumb_url
    };

    // Extract title
    let title = {
        let selectors = vec!["div#title-posta h2 a"];

        let mut page_title = None;
        for selector in selectors {
            if let Ok(element) = client.find(Locator::Css(selector)).await {
                if let Ok(text) = element.text().await {
                    if !text.trim().is_empty() {
                        page_title = Some(text.trim().to_string());
                        break;
                    }
                }
            }
        }
        page_title
    };

    (thumbnail, title)
}

async fn process_page_elements_parallel(
    hrefs_and_elements: Vec<(String, Element)>,
    page_count: i32,
    is_first_page: bool,
    processed_urls: &Arc<Mutex<HashSet<String>>>,
    max_concurrent: usize,
) -> Result<Vec<Value>, fantoccini::error::CmdError> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut tasks = Vec::new();

    for (element_index, (href, _element)) in hrefs_and_elements.into_iter().enumerate() {
        let element_index = element_index + 1;

        // Skip first element only on first page
        if is_first_page && element_index == 1 {
            println!("⭐ Skipping first element on first page");
            let mut urls = processed_urls.lock().await;
            urls.insert(href);
            continue;
        }

        // Mark as processed immediately
        {
            let mut urls = processed_urls.lock().await;
            urls.insert(href.clone());
        }

        let semaphore_clone = semaphore.clone();
        let href_clone = href.clone();

        let task = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();

            // Create a new browser client for this task
            let client = match create_browser_client().await {
                Ok(client) => client,
                Err(e) => {
                    println!(
                        "❌ Failed to create browser client for element {}: {:?}",
                        element_index, e
                    );
                    return None;
                }
            };

            let (thumbnail, title) = extract_thumbnail_and_title(&client).await;
            // Navigate to the specific URL directly
            if let Err(e) = client.goto(&href_clone).await {
                println!("❌ Failed to navigate to URL {}: {:?}", href_clone, e);
                let _ = client.close().await;
                return None;
            }

            // Extract thumbnail and title

            // Process video sources
            let mut src_list = Vec::new();
            let source_selectors = vec![
                "div#content div#content-inside div#main-single source",
                "video source",
                "video[src]",
                "source[src]",
            ];

            for selector in source_selectors {
                if let Ok(sources) = client.find_all(Locator::Css(selector)).await {
                    for source in sources {
                        if let Ok(Some(src)) = source.attr("src").await {
                            if !src.is_empty() && (src.starts_with("http") || src.starts_with("//"))
                            {
                                let normalized_src = if src.starts_with("//") {
                                    format!("https:{}", src)
                                } else {
                                    src
                                };

                                if !src_list.contains(&normalized_src) {
                                    src_list.push(normalized_src);
                                }
                            }
                        }
                    }
                }
            }

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let item_data = json!({
                "page": page_count,
                "element_index": element_index,
                "url": href_clone,
                "title": title,
                "thumbnail": thumbnail,
                "sources": src_list,
                "timestamp": timestamp
            });

            // Close the client
            let _ = client.close().await;

            println!(
                "✅ Processed element {} ({}) - Title: {:?}",
                element_index, href_clone, title
            );
            Some(item_data)
        });

        tasks.push(task);
    }

    println!(
        "🚀 Starting {} parallel tasks with {} CPU cores",
        tasks.len(),
        max_concurrent
    );

    // Wait for all tasks
    let mut all_data = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Some(data)) => all_data.push(data),
            Ok(None) => {} // Skipped or failed element
            Err(e) => println!("❌ Task failed: {:?}", e),
        }
    }

    Ok(all_data)
}

async fn scraping_task(state: ScrapingState) -> Result<(), fantoccini::error::CmdError> {
    let cpu_cores = num_cpus::get();
    println!("💪 Detected {} CPU cores, using all of them!", cpu_cores);

    // Create main client for navigation only
    let main_client = create_browser_client().await?;
    main_client.goto("https://zzxxtra.com/").await?;

    let mut page_count = 1;
    let mut is_first_page = true;
    let mut all_data: Vec<Value> = Vec::new();
    let processed_urls = Arc::new(Mutex::new(HashSet::new()));

    loop {
        println!("🚀 Processing page: {}", page_count);

        // Get all elements on current page
        let elements = main_client
            .find_all(Locator::Css("div#main div.multiple a:not(#starrings a)"))
            .await?;
        println!(
            "⚡ Found {} elements on page {}",
            elements.len(),
            page_count
        );

        // Pre-collect all hrefs and filter duplicates
        let mut hrefs_and_elements = Vec::new();
        let mut temp_seen_on_page = HashSet::new();
        let processed_urls_guard = processed_urls.lock().await;

        for element in elements {
            if let Ok(Some(href)) = element.attr("href").await {
                if !processed_urls_guard.contains(&href)
                    && !temp_seen_on_page.contains(&href)
                    && !href.contains("/tag/")
                {
                    temp_seen_on_page.insert(href.clone());
                    hrefs_and_elements.push((href, element));
                }
            }
        }

        drop(processed_urls_guard);
        println!(
            "💨 Processing {} unique links with {} CPU cores",
            hrefs_and_elements.len(),
            cpu_cores
        );

        // Process elements in parallel using all CPU cores
        let mut page_data = process_page_elements_parallel(
            hrefs_and_elements,
            page_count,
            is_first_page,
            &processed_urls,
            cpu_cores,
        )
        .await?;

        all_data.append(&mut page_data);
        is_first_page = false;

        // Update state after each page
        let intermediate_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let intermediate_info = json!({
            "pages_processed_so_far": page_count,
            "items_found_so_far": all_data.len(),
            "last_update_timestamp": intermediate_timestamp,
            "website": "https://zzxxtra.com/",
            "cpu_cores_used": cpu_cores,
            "status": "in_progress"
        });

        // Update shared state
        {
            let mut data = state.data.lock().await;
            *data = all_data.clone();
        }
        {
            let mut info = state.info.lock().await;
            *info = intermediate_info;
        }

        println!(
            "💾 Updated state: {} items from {} pages",
            all_data.len(),
            page_count
        );

        // Navigate to next page
        let next_button_found = match main_client.find(Locator::Css("a.nextpostslink")).await {
            Ok(next_button) => match next_button.click().await {
                Ok(_) => {
                    println!("🚀 Clicked next page button");
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    true
                }
                Err(e) => {
                    println!("❌ Failed to click next button: {:?}", e);
                    false
                }
            },
            Err(_) => {
                println!("🏁 No more pages - reached the end");
                false
            }
        };

        if !next_button_found {
            break;
        }

        page_count += 1;
    }

    // Close main client
    main_client.close().await?;

    // Final update
    let final_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let final_info = json!({
        "total_pages_processed": page_count,
        "total_items_found": all_data.len(),
        "scraping_timestamp": final_timestamp,
        "website": "https://zzxxtra.com/",
        "cpu_cores_used": cpu_cores,
        "status": "completed"
    });

    {
        let mut info = state.info.lock().await;
        *info = final_info;
    }

    println!(
        "🎉 MEGA ULTRA SPEED SCRAPING WITH {} CPU CORES COMPLETED!",
        cpu_cores
    );
    println!("📊 Processed {} pages total", page_count);
    println!("📦 Found {} unique items", all_data.len());

    Ok(())
}

// API endpoint to get current data
async fn get_data(state: ScrapingState) -> Result<impl warp::Reply, warp::Rejection> {
    let data = state.data.lock().await;
    let info = state.info.lock().await;

    let response = json!({
        "scraping_info": info.clone(),
        "data": data.clone()
    });

    Ok(warp::reply::json(&response))
}

// Serve the HTML file
async fn serve_html() -> Result<impl warp::Reply, warp::Rejection> {
    let html_content = include_str!("../index.html");
    Ok(warp::reply::html(html_content))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpu_cores = num_cpus::get();
    println!("🚀 Starting Enhanced Web Scraper Service");
    println!("💪 Detected {} CPU cores", cpu_cores);
    println!("🌐 Web interface will be available at: http://localhost:3030");

    // Initialize shared state
    let scraping_state = ScrapingState {
        data: Arc::new(Mutex::new(Vec::new())),
        info: Arc::new(Mutex::new(json!({
            "status": "initializing",
            "cpu_cores_used": cpu_cores,
            "website": "https://zzxxtra.com/"
        }))),
    };

    // Clone state for the scraping task
    let scraping_state_clone = scraping_state.clone();

    // Start scraping in background
    tokio::spawn(async move {
        if let Err(e) = scraping_task(scraping_state_clone).await {
            println!("❌ Scraping task failed: {:?}", e);
        }
    });

    // Setup web routes
    let state_filter = warp::any().map(move || scraping_state.clone());

    let html_route = warp::path::end().and(warp::get()).and_then(serve_html);

    let api_route = warp::path("api")
        .and(warp::path("data"))
        .and(warp::get())
        .and(state_filter)
        .and_then(get_data);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET", "POST"]);

    let routes = html_route.or(api_route).with(cors);

    println!("✅ Web server started on http://localhost:3030");
    println!(
        "🔄 Scraping started in background with {} CPU cores",
        cpu_cores
    );

    warp::serve(routes).run(([0, 0, 0, 0], 3030)).await;

    Ok(())
}
