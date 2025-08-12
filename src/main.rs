use fantoccini::elements::Element;
use fantoccini::{ClientBuilder, Locator};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio;
use tokio::sync::{Mutex, Semaphore};

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
            "--disable-images",
            "--disable-javascript",
            "--disable-css",
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

async fn process_single_element(
    client: &fantoccini::Client,
    href: String,
    element_index: usize,
    page_count: i32,
    element: Element,
) -> Result<Option<Value>, fantoccini::error::CmdError> {
    match element.click().await {
        Ok(_) => {
            tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

            let url = client.current_url().await?;
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
                "url": url.to_string(),
                "sources": src_list,
                "timestamp": timestamp
            });

            client.back().await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

            println!("✅ Processed element {} ({})", element_index, href);
            Ok(Some(item_data))
        }
        Err(e) => {
            println!("❌ Failed to click element {}: {:?}", element_index, e);
            Ok(None)
        }
    }
}

async fn process_page_elements_parallel(
    hrefs_and_elements: Vec<(String, Element)>,
    page_count: i32,
    is_first_page: bool,
    processed_urls: &Arc<Mutex<HashSet<String>>>,
) -> Result<Vec<Value>, fantoccini::error::CmdError> {
    const MAX_CONCURRENT: usize = 12; // 12 rdzeni CPU
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut tasks = Vec::new();

    for (element_index, (href, _element)) in hrefs_and_elements.into_iter().enumerate() {
        let element_index = element_index + 1;

        // Skip first element only on first page
        if is_first_page && element_index == 1 {
            println!("⏭️  Skipping first element on first page");
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

            // Navigate to the specific URL directly instead of searching for element
            if let Err(e) = client.goto(&href_clone).await {
                println!("❌ Failed to navigate to URL {}: {:?}", href_clone, e);
                let _ = client.close().await;
                return None;
            }

            // Process the page directly
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
                "sources": src_list,
                "timestamp": timestamp
            });

            // Close the client
            let _ = client.close().await;

            println!("✅ Processed element {} ({})", element_index, href_clone);
            Some(item_data)
        });

        tasks.push(task);
    }

    println!(
        "🚀 Starting {} parallel tasks with {} CPU cores",
        tasks.len(),
        MAX_CONCURRENT
    );

    // Czekamy na wszystkie zadania bez futures::join_all
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

#[tokio::main]
async fn main() -> Result<(), fantoccini::error::CmdError> {
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
            "💨 Processing {} unique links with 12 CPU cores",
            hrefs_and_elements.len()
        );

        // Process elements in parallel using 12 CPU cores
        let mut page_data = process_page_elements_parallel(
            hrefs_and_elements,
            page_count,
            is_first_page,
            &processed_urls,
        )
        .await?;

        all_data.append(&mut page_data);
        is_first_page = false;

        // 💾 ZAPISZ DANE PO KAŻDEJ STRONIE
        let intermediate_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let intermediate_json = json!({
            "scraping_info": {
                "pages_processed_so_far": page_count,
                "items_found_so_far": all_data.len(),
                "last_update_timestamp": intermediate_timestamp,
                "website": "https://zzxxtra.com/",
                "cpu_cores_used": 12,
                "status": "in_progress"
            },
            "data": all_data
        });

        let json_string = serde_json::to_string(&intermediate_json).unwrap();
        std::fs::write("scraped_data.json", json_string).expect("Could not write file");

        println!(
            "💾 Saved {} items from {} pages to scraped_data.json",
            all_data.len(),
            page_count
        );

        println!(
            "🏁 Finished page {}, looking for next button...",
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

    // Save results
    let final_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let final_json = json!({
        "scraping_info": {
            "total_pages_processed": page_count,
            "total_items_found": all_data.len(),
            "scraping_timestamp": final_timestamp,
            "website": "https://zzxxtra.com/",
            "cpu_cores_used": 12,
            "status": "completed"
        },
        "data": all_data
    });

    let json_string = serde_json::to_string(&final_json).unwrap();
    std::fs::write("scraped_data.json", json_string).expect("Could not write file");

    println!("🎉 MEGA ULTRA SPEED SCRAPING WITH 12 CPU CORES COMPLETED!");
    println!("📊 Processed {} pages total", page_count);
    println!("📦 Found {} unique items", all_data.len());

    let processed_count = processed_urls.lock().await.len();
    println!("🔗 Processed {} unique URLs", processed_count);
    println!("💾 Data saved to scraped_data.json");

    Ok(())
}
