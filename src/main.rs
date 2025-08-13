use dotenv::dotenv;
use fantoccini::{ClientBuilder, Locator};
use futures::TryFutureExt;
use num_cpus;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use std::env;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio;
use tokio::sync::{Mutex, Semaphore};
use warp::Filter;

// Dodajemy strukturę dla danych z bazy
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
struct ScrapedItem {
    id: Option<i32>,
    page: i32,
    element_index: i32,
    url: String,
    title: Option<String>,
    thumbnail: Option<String>,
    sources: Option<serde_json::Value>, // Przechowujemy jako JSONB
    timestamp: i64,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
}

// Struktura do przechowywania danych scraping'u
#[derive(Clone)]
struct ScrapingState {
    data: Arc<Mutex<Vec<Value>>>,
    info: Arc<Mutex<Value>>,
    db_pool: PgPool,
}

// Struktura dla danych pobranych przed kliknięciem
#[derive(Debug, Clone)]
struct PreClickData {
    href: String,
    thumbnail: Option<String>,
    title: Option<String>,
}

// Nowa struktura dla informacji o stronie
#[derive(Debug)]
struct PageInfo {
    page_number: i32,
    elements_count: i32,
    is_complete: bool,
}

// Inicjalizacja bazy danych
async fn init_database(database_url: &str) -> Result<PgPool, sqlx::Error> {
    println!("🗄️  Connecting to PostgreSQL database...");
    println!(
        "📋 Database URL: {}",
        database_url
            .chars()
            .enumerate()
            .map(|(i, c)| if i > 20 && i < database_url.len() - 10 {
                '*'
            } else {
                c
            })
            .collect::<String>()
    );

    // Konfiguracja pool connection z większymi timeoutami
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5) // Ograniczamy liczbę połączeń
        .min_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(30))
        .idle_timeout(std::time::Duration::from_secs(600))
        .max_lifetime(std::time::Duration::from_secs(1800))
        .connect(database_url)
        .await?;

    // Testujemy połączenie
    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => println!("✅ Database connection test successful"),
        Err(e) => {
            println!("❌ Database connection test failed: {:?}", e);
            return Err(e);
        }
    }

    // Tworzymy tabelę jeśli nie istnieje - każde polecenie osobno
    match sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS scraped_items (
            id SERIAL PRIMARY KEY,
            page INTEGER NOT NULL,
            element_index INTEGER NOT NULL,
            url TEXT NOT NULL UNIQUE,
            title TEXT,
            thumbnail TEXT,
            sources JSONB,
            timestamp BIGINT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        "#,
    )
    .execute(&pool)
    .await
    {
        Ok(_) => println!("✅ Table created/verified successfully"),
        Err(e) => {
            println!("❌ Failed to create table: {:?}", e);
            return Err(e);
        }
    }

    // Dodajemy tabelę dla śledzenia ukończonych stron
    match sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS page_completion (
            id SERIAL PRIMARY KEY,
            page_number INTEGER NOT NULL UNIQUE,
            elements_count INTEGER NOT NULL,
            completed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            is_complete BOOLEAN DEFAULT true
        )
        "#,
    )
    .execute(&pool)
    .await
    {
        Ok(_) => println!("✅ Page completion table created/verified successfully"),
        Err(e) => {
            println!("❌ Failed to create page completion table: {:?}", e);
            return Err(e);
        }
    }

    // Tworzymy indeksy osobno - z obsługą błędów
    let indexes = vec![
        (
            "idx_scraped_items_url",
            "CREATE INDEX IF NOT EXISTS idx_scraped_items_url ON scraped_items(url)",
        ),
        (
            "idx_scraped_items_page",
            "CREATE INDEX IF NOT EXISTS idx_scraped_items_page ON scraped_items(page)",
        ),
        (
            "idx_scraped_items_timestamp",
            "CREATE INDEX IF NOT EXISTS idx_scraped_items_timestamp ON scraped_items(timestamp)",
        ),
        (
            "idx_page_completion_page",
            "CREATE INDEX IF NOT EXISTS idx_page_completion_page ON page_completion(page_number)",
        ),
    ];

    for (name, query) in indexes {
        match sqlx::query(query).execute(&pool).await {
            Ok(_) => println!("✅ Index {} created/verified", name),
            Err(e) => {
                println!("⚠️ Warning: Failed to create index {}: {:?}", name, e);
                // Nie przerywamy na błędach indeksów - mogą już istnieć
            }
        }
    }

    println!("✅ Database initialized successfully");
    Ok(pool)
}

// Funkcja do sprawdzania które strony są już ukończone
async fn get_completed_pages(pool: &PgPool) -> Result<HashSet<i32>, sqlx::Error> {
    let completed_pages: Vec<i32> = sqlx::query_scalar(
        "SELECT page_number FROM page_completion WHERE is_complete = true ORDER BY page_number",
    )
    .fetch_all(pool)
    .await?;

    println!(
        "📋 Found {} completed pages: {:?}",
        completed_pages.len(),
        completed_pages
    );
    Ok(completed_pages.into_iter().collect())
}

// Funkcja do znajdowania następnej strony do scrapowania
async fn find_next_page_to_scrape(pool: &PgPool) -> Result<i32, sqlx::Error> {
    // Znajdź najwyższą ukończoną stronę
    let highest_completed: Option<i32> =
        sqlx::query_scalar("SELECT MAX(page_number) FROM page_completion WHERE is_complete = true")
            .fetch_one(pool)
            .await?;

    let next_page = match highest_completed {
        Some(max_page) => {
            // Sprawdź czy są dziury w numeracji stron
            let missing_page: Option<i32> = sqlx::query_scalar(
                r#"
                SELECT t1.page_number + 1 as missing_page
                FROM page_completion t1
                LEFT JOIN page_completion t2 ON t1.page_number + 1 = t2.page_number
                WHERE t2.page_number IS NULL 
                AND t1.page_number < $1
                ORDER BY t1.page_number
                LIMIT 1
                "#,
            )
            .bind(max_page)
            .fetch_optional(pool)
            .await?;

            missing_page.unwrap_or(max_page + 1)
        }
        None => 1, // Brak ukończonych stron, zacznij od 1
    };

    println!("🎯 Next page to scrape: {}", next_page);
    Ok(next_page)
}

// Funkcja do sprawdzania czy strona jest kompletna
async fn check_page_completion(
    pool: &PgPool,
    page_number: i32,
    expected_elements: i32,
) -> Result<bool, sqlx::Error> {
    // Sprawdź ile elementów mamy w bazie dla tej strony
    let actual_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM scraped_items WHERE page = $1")
            .bind(page_number)
            .fetch_one(pool)
            .await?;

    let is_complete = actual_count as i32 >= expected_elements;

    println!(
        "📊 Page {} completion check: {} elements found, {} expected, complete: {}",
        page_number, actual_count, expected_elements, is_complete
    );

    Ok(is_complete)
}

// Funkcja do oznaczania strony jako ukończonej
async fn mark_page_complete(
    pool: &PgPool,
    page_number: i32,
    elements_count: i32,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO page_completion (page_number, elements_count, is_complete)
        VALUES ($1, $2, true)
        ON CONFLICT (page_number) 
        DO UPDATE SET 
            elements_count = EXCLUDED.elements_count,
            is_complete = true,
            completed_at = NOW()
        "#,
    )
    .bind(page_number)
    .bind(elements_count)
    .execute(pool)
    .await?;

    println!(
        "✅ Marked page {} as complete with {} elements",
        page_number, elements_count
    );
    Ok(())
}

// Funkcja do pobierania wszystkich URL-ów z bazy (dla sprawdzania duplikatów)
async fn get_all_scraped_urls(pool: &PgPool) -> Result<HashSet<String>, sqlx::Error> {
    let urls: Vec<String> = sqlx::query_scalar("SELECT url FROM scraped_items")
        .fetch_all(pool)
        .await?;

    println!("🔍 Loaded {} existing URLs from database", urls.len());
    Ok(urls.into_iter().collect())
}

// Dodawanie danych do bazy - z retry logic
async fn save_item_to_db(pool: &PgPool, item: &Value) -> Result<i32, sqlx::Error> {
    let sources_json = item["sources"].clone();

    // Retry logic - próbujemy 3 razy
    let mut attempts = 0;
    let max_attempts = 3;

    while attempts < max_attempts {
        attempts += 1;

        match sqlx::query(
            r#"
            INSERT INTO scraped_items (page, element_index, url, title, thumbnail, sources, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (url) DO UPDATE SET
                title = EXCLUDED.title,
                thumbnail = EXCLUDED.thumbnail,
                sources = EXCLUDED.sources,
                timestamp = EXCLUDED.timestamp
            RETURNING id
            "#,
        )
        .bind(item["page"].as_i64().unwrap_or(0) as i32)
        .bind(item["element_index"].as_i64().unwrap_or(0) as i32)
        .bind(item["url"].as_str().unwrap_or(""))
        .bind(item["title"].as_str())
        .bind(item["thumbnail"].as_str())
        .bind(sources_json.clone())
        .bind(item["timestamp"].as_i64().unwrap_or(0))
        .fetch_one(pool)
        .await {
            Ok(row) => return Ok(row.get("id")),
            Err(e) => {
                if attempts >= max_attempts {
                    return Err(e);
                }
                println!("⚠️ Database save attempt {} failed: {:?}, retrying...", attempts, e);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000 * attempts as u64)).await;
            }
        }
    }

    // This should never be reached due to the loop logic above
    Err(sqlx::Error::PoolTimedOut)
}

// Pobieranie danych z bazy
async fn get_items_from_db(
    pool: &PgPool,
    limit: Option<i32>,
    offset: Option<i32>,
) -> Result<Vec<ScrapedItem>, sqlx::Error> {
    let limit = limit.unwrap_or(100);
    let offset = offset.unwrap_or(0);

    let items = sqlx::query_as::<_, ScrapedItem>(
        r#"
        SELECT id, page, element_index, url, title, thumbnail, sources, timestamp, created_at
        FROM scraped_items
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await?;

    Ok(items)
}

// Pobieranie statystyk z bazy
async fn get_db_stats(pool: &PgPool) -> Result<Value, sqlx::Error> {
    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM scraped_items")
        .fetch_one(pool)
        .await?;

    let pages_count: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT page) FROM scraped_items")
        .fetch_one(pool)
        .await?;

    let completed_pages_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM page_completion WHERE is_complete = true")
            .fetch_one(pool)
            .await?;

    let latest_timestamp: Option<i64> =
        sqlx::query_scalar("SELECT MAX(timestamp) FROM scraped_items")
            .fetch_one(pool)
            .await?;

    Ok(json!({
        "total_items": total_count,
        "total_pages": pages_count,
        "completed_pages": completed_pages_count,
        "latest_timestamp": latest_timestamp,
        "database_status": "connected"
    }))
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
    let url = env::var("chrome_url").unwrap_or("localhost:4444".to_string());
    let client = ClientBuilder::native()
        .capabilities(caps)
        .connect(&url)
        .await
        .expect("Failed to connect to WebDriver");

    client.set_window_rect(0, 0, 1280, 720).await?;
    Ok(client)
}

async fn extract_pre_click_data(
    client: &fantoccini::Client,
) -> Result<Vec<PreClickData>, fantoccini::error::CmdError> {
    let mut pre_click_data = Vec::new();

    let elements = client
        .find_all(Locator::Css("div#main div.multiple"))
        .await?;

    println!(
        "🔍 Extracting pre-click data from {} elements",
        elements.len()
    );

    for (_index, element) in elements.iter().enumerate() {
        let anchors = element.find_all(Locator::Css("a")).await?;

        if anchors.len() < 2 {
            continue;
        }

        let target_a = &anchors[anchors.len() - 2];

        let href = match target_a.attr("href").await {
            Ok(Some(href)) => href,
            _ => continue,
        };

        let title = target_a.attr("title").await?.unwrap_or_default();

        let thumbnail = match target_a.find(Locator::Css("img")).await {
            Ok(img_elem) => img_elem.attr("src").await?.unwrap_or_default(),
            _ => String::new(),
        };

        pre_click_data.push(PreClickData {
            href: href,
            title: Some(title),
            thumbnail: Some(thumbnail),
        });
    }

    Ok(pre_click_data)
}

async fn extract_video_sources_from_page(client: &fantoccini::Client) -> Vec<String> {
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
                    if !src.is_empty() && (src.starts_with("http") || src.starts_with("//")) {
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

    src_list
}

async fn process_page_elements_parallel(
    pre_click_data: Vec<PreClickData>,
    page_count: i32,
    is_first_page: bool,
    processed_urls: &Arc<Mutex<HashSet<String>>>,
    max_concurrent: usize,
    db_pool: &PgPool,
) -> Result<Vec<Value>, fantoccini::error::CmdError> {
    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut tasks = Vec::new();

    for (element_index, data) in pre_click_data.into_iter().enumerate() {
        let element_index = element_index + 1;

        if is_first_page && element_index == 1 {
            println!("⭐ Skipping first element on first page");
            let mut urls = processed_urls.lock().await;
            urls.insert(data.href);
            continue;
        }

        {
            let mut urls = processed_urls.lock().await;
            urls.insert(data.href.clone());
        }

        let semaphore_clone = semaphore.clone();
        let data_clone = data.clone();
        let db_pool_clone = db_pool.clone();

        let task = tokio::spawn(async move {
            let _permit = semaphore_clone.acquire().await.unwrap();

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

            if let Err(e) = client.goto(&data_clone.href).await {
                println!("❌ Failed to navigate to URL {}: {:?}", data_clone.href, e);
                let _ = client.close().await;
                return None;
            }

            let src_list = extract_video_sources_from_page(&client).await;

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let item_data = json!({
                "page": page_count,
                "element_index": element_index,
                "url": data_clone.href,
                "title": data_clone.title,
                "thumbnail": data_clone.thumbnail,
                "sources": src_list,
                "timestamp": timestamp
            });

            // Zapisz do bazy danych
            match save_item_to_db(&db_pool_clone, &item_data).await {
                Ok(id) => {
                    println!("💾 Saved to database with ID: {}", id);
                }
                Err(e) => {
                    println!("❌ Failed to save to database: {:?}", e);
                }
            }

            let _ = client.close().await;

            println!(
                "✅ Processed element {} ({}) - Title: {:?}, Thumbnail: {:?}, Sources: {}",
                element_index,
                data_clone.href,
                data_clone.title,
                data_clone
                    .thumbnail
                    .as_ref()
                    .map(|t| &t[..std::cmp::min(30, t.len())]),
                src_list.len()
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

    let mut all_data = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Some(data)) => all_data.push(data),
            Ok(None) => {}
            Err(e) => println!("❌ Task failed: {:?}", e),
        }
    }

    Ok(all_data)
}

// Funkcja do nawigacji do konkretnej strony
async fn navigate_to_page(
    client: &fantoccini::Client,
    target_page: i32,
) -> Result<(), fantoccini::error::CmdError> {
    if target_page == 1 {
        // Już jesteśmy na pierwszej stronie
        return Ok(());
    }

    println!("🎯 Navigating to page {}", target_page);

    // Najpierw sprawdźmy czy możemy przejść bezpośrednio przez URL
    let page_url = if target_page == 1 {
        "https://zzxxtra.com/".to_string()
    } else {
        format!("https://zzxxtra.com/page/{}/", target_page)
    };

    client.goto(&page_url).await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    // Sprawdź czy strona się załadowała poprawnie
    match client.find(Locator::Css("div#main div.multiple")).await {
        Ok(elements) => {
            if elements.text().await?.trim().is_empty() {
                return Err(fantoccini::error::CmdError::NotW3C(
                    serde_json::Value::String("Page not found or empty".to_string()),
                ));
            }
            println!("✅ Successfully navigated to page {}", target_page);
            Ok(())
        }
        Err(e) => {
            println!("❌ Failed to find content on page {}: {:?}", target_page, e);
            Err(e)
        }
    }
}

async fn scraping_task(state: ScrapingState) -> Result<(), fantoccini::error::CmdError> {
    let cpu_cores = num_cpus::get();
    println!("💪 Detected {} CPU cores, using all of them!", cpu_cores);

    // Sprawdź które strony są już ukończone
    let completed_pages = match get_completed_pages(&state.db_pool).await {
        Ok(pages) => pages,
        Err(e) => {
            println!("❌ Failed to get completed pages: {:?}", e);
            HashSet::new()
        }
    };

    // Znajdź następną stronę do scrapowania
    let start_page = match find_next_page_to_scrape(&state.db_pool).await {
        Ok(page) => page,
        Err(e) => {
            println!(
                "❌ Failed to determine starting page: {:?}, starting from page 1",
                e
            );
            1
        }
    };

    println!("🚀 Starting scraping from page: {}", start_page);
    println!("📋 Already completed pages: {:?}", completed_pages);

    // Załaduj wszystkie istniejące URL-e z bazy
    let processed_urls = Arc::new(Mutex::new(
        match get_all_scraped_urls(&state.db_pool).await {
            Ok(urls) => urls,
            Err(e) => {
                println!("❌ Failed to load existing URLs: {:?}", e);
                HashSet::new()
            }
        },
    ));

    let main_client = create_browser_client().await?;
    main_client.goto("https://zzxxtra.com/").await?;

    let mut page_count = start_page;
    let mut all_data: Vec<Value> = Vec::new();

    // Jeśli nie zaczynamy od strony 1, nawiguj do właściwej strony
    if start_page > 1 {
        if let Err(e) = navigate_to_page(&main_client, start_page).await {
            println!("❌ Failed to navigate to page {}: {:?}", start_page, e);
            println!("🔄 Starting from page 1 instead");
            page_count = 1;
            main_client.goto("https://zzxxtra.com/").await?;
        }
    }

    loop {
        // Sprawdź czy ta strona jest już ukończona
        if completed_pages.contains(&page_count) {
            println!("⏭️  Page {} already completed, skipping", page_count);

            // Przejdź do następnej strony
            let next_button_found = match main_client.find(Locator::Css("a.nextpostslink")).await {
                Ok(next_button) => match next_button.click().await {
                    Ok(_) => {
                        println!("🚀 Clicked next page button (skipping completed page)");
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
            continue;
        }

        println!("🚀 Processing page: {}", page_count);

        let pre_click_data = extract_pre_click_data(&main_client).await?;

        println!(
            "📋 Extracted pre-click data for {} elements on page {}",
            pre_click_data.len(),
            page_count
        );

        // Sprawdź czy ta strona została już częściowo przetworzona
        let elements_in_db = match sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM scraped_items WHERE page = $1",
        )
        .bind(page_count)
        .fetch_one(&state.db_pool)
        .await
        {
            Ok(count) => count as i32,
            Err(e) => {
                println!("❌ Failed to check existing elements: {:?}", e);
                0
            }
        };

        println!(
            "📊 Page {} - Found {} elements on page, {} already in database",
            page_count,
            pre_click_data.len(),
            elements_in_db
        );

        let mut filtered_data = Vec::new();
        let mut temp_seen_on_page = HashSet::new();
        let processed_urls_guard = processed_urls.lock().await;

        for data in pre_click_data.iter() {
            if !processed_urls_guard.contains(&data.href)
                && !temp_seen_on_page.contains(&data.href)
                && !data.href.contains("/tag/")
            {
                temp_seen_on_page.insert(data.href.clone());
                filtered_data.push(data.clone());
            }
        }

        drop(processed_urls_guard);

        // Jeśli nie ma nowych elementów do przetworzenia, oznacz stronę jako ukończoną
        if filtered_data.is_empty() && elements_in_db > 0 {
            println!(
                "✅ Page {} appears to be complete ({} elements in DB), marking as complete",
                page_count, elements_in_db
            );

            if let Err(e) = mark_page_complete(&state.db_pool, page_count, elements_in_db).await {
                println!("❌ Failed to mark page as complete: {:?}", e);
            }
        } else if !filtered_data.is_empty() {
            println!(
                "💨 Processing {} new unique links with {} CPU cores",
                filtered_data.len(),
                cpu_cores
            );

            let is_first_page = page_count == 1 && start_page == 1;
            let mut page_data = process_page_elements_parallel(
                filtered_data,
                page_count,
                is_first_page,
                &processed_urls,
                cpu_cores,
                &state.db_pool,
            )
            .await?;

            all_data.append(&mut page_data);

            // Sprawdź czy strona jest teraz kompletna
            let total_elements_on_page = pre_click_data.len() as i32;
            let total_elements_in_db = match sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM scraped_items WHERE page = $1",
            )
            .bind(page_count)
            .fetch_one(&state.db_pool)
            .await
            {
                Ok(count) => count as i32,
                Err(_) => 0,
            };

            // Sprawdź czy strona jest kompletna (uwzględniając że pierwszy element pierwszej strony jest pomijany)
            let expected_elements = if page_count == 1 && start_page == 1 {
                total_elements_on_page - 1 // Pierwszy element jest pomijany na pierwszej stronie
            } else {
                total_elements_on_page
            };

            if total_elements_in_db >= expected_elements {
                println!(
                    "✅ Page {} is now complete ({}/{} elements), marking as complete",
                    page_count, total_elements_in_db, expected_elements
                );

                if let Err(e) =
                    mark_page_complete(&state.db_pool, page_count, total_elements_in_db).await
                {
                    println!("❌ Failed to mark page as complete: {:?}", e);
                }
            } else {
                println!(
                    "⏳ Page {} still incomplete ({}/{} elements)",
                    page_count, total_elements_in_db, expected_elements
                );
            }
        }

        // Pobierz statystyki z bazy i aktualizuj stan
        let db_stats = match get_db_stats(&state.db_pool).await {
            Ok(stats) => stats,
            Err(e) => {
                println!("❌ Failed to get DB stats: {:?}", e);
                json!({})
            }
        };

        let intermediate_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let intermediate_info = json!({
            "pages_processed_so_far": page_count,
            "items_found_so_far": all_data.len(),
            "current_page": page_count,
            "last_update_timestamp": intermediate_timestamp,
            "website": "https://zzxxtra.com/",
            "cpu_cores_used": cpu_cores,
            "status": "in_progress",
            "database_stats": db_stats,
            "resume_info": {
                "started_from_page": start_page,
                "completed_pages_count": completed_pages.len()
            }
        });

        {
            let mut data = state.data.lock().await;
            *data = all_data.clone();
        }
        {
            let mut info = state.info.lock().await;
            *info = intermediate_info;
        }

        println!(
            "💾 Updated state: {} items from {} pages (current: {})",
            all_data.len(),
            page_count,
            page_count
        );

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

    main_client.close().await?;

    let db_stats = get_db_stats(&state.db_pool)
        .await
        .unwrap_or_else(|_| json!({}));
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
        "status": "completed",
        "database_stats": db_stats,
        "resume_info": {
            "started_from_page": start_page,
            "ended_at_page": page_count,
            "completed_pages_count": completed_pages.len()
        }
    });

    {
        let mut info = state.info.lock().await;
        *info = final_info;
    }

    println!(
        "🎉 MEGA ULTRA SPEED SCRAPING WITH {} CPU CORES COMPLETED!",
        cpu_cores
    );
    println!(
        "📊 Started from page {}, processed up to page {}",
        start_page, page_count
    );
    println!("📦 Found {} unique items in this session", all_data.len());
    println!(
        "✅ Resume functionality enabled - next restart will continue from where we left off!"
    );

    Ok(())
}

// API endpoint to get current data (in-memory)
async fn get_data(state: ScrapingState) -> Result<impl warp::Reply, warp::Rejection> {
    let data = state.data.lock().await;
    let info = state.info.lock().await;

    let response = json!({
        "scraping_info": info.clone(),
        "data": data.clone()
    });

    Ok(warp::reply::json(&response))
}

// API endpoint to get data from PostgreSQL
async fn get_db_data(
    state: ScrapingState,
    query_params: std::collections::HashMap<String, String>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let limit = query_params
        .get("limit")
        .and_then(|s| s.parse::<i32>().ok());

    let offset = query_params
        .get("offset")
        .and_then(|s| s.parse::<i32>().ok());

    match get_items_from_db(&state.db_pool, limit, offset).await {
        Ok(items) => {
            let stats = get_db_stats(&state.db_pool)
                .await
                .unwrap_or_else(|_| json!({}));

            let response = json!({
                "database_stats": stats,
                "items": items,
                "query_params": {
                    "limit": limit.unwrap_or(100),
                    "offset": offset.unwrap_or(0)
                }
            });

            Ok(warp::reply::json(&response))
        }
        Err(e) => {
            let error_response = json!({
                "error": "Database query failed",
                "details": e.to_string()
            });
            Ok(warp::reply::json(&error_response))
        }
    }
}

// API endpoint to get resume information
async fn get_resume_info(state: ScrapingState) -> Result<impl warp::Reply, warp::Rejection> {
    let completed_pages = get_completed_pages(&state.db_pool)
        .await
        .unwrap_or_else(|_| HashSet::new());

    let next_page = find_next_page_to_scrape(&state.db_pool).await.unwrap_or(1);

    let stats = get_db_stats(&state.db_pool)
        .await
        .unwrap_or_else(|_| json!({}));

    let response = json!({
        "completed_pages": completed_pages.into_iter().collect::<Vec<_>>(),
        "next_page_to_scrape": next_page,
        "database_stats": stats,
        "can_resume": true
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
    dotenv().ok();
    let cpu_cores = num_cpus::get();
    println!("🚀 Starting Enhanced Web Scraper Service with PostgreSQL & Resume Functionality");
    println!("💪 Detected {} CPU cores", cpu_cores);
    println!("🔄 Resume functionality: Will automatically continue from last incomplete page");
    println!("🌐 Web interface will be available at: http://localhost:3030");

    // Database connection string - z lepszą obsługą błędów
    let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
        println!("⚠️ DATABASE_URL not set, using default");
        "postgresql://rust:Zu7G9BNbcPonlDaWJ7dNHbtoRbs0hX4D@dpg-d2e4unqdbo4c73en5j70-a.oregon-postgres.render.com/rys".to_string()
    });

    // Initialize database with error handling
    let db_pool = match init_database(&database_url).await {
        Ok(pool) => {
            println!("✅ Database connected successfully");
            pool
        }
        Err(e) => {
            println!("❌ Database connection failed: {:?}", e);
            println!("🔄 Resume functionality requires database connection");
            return Err(format!("Database connection failed: {:?}", e).into());
        }
    };

    // Show resume information on startup
    match get_completed_pages(&db_pool).await {
        Ok(completed) => {
            let next_page = find_next_page_to_scrape(&db_pool).await.unwrap_or(1);
            println!(
                "📋 Resume info: {} completed pages, will start from page {}",
                completed.len(),
                next_page
            );
            if !completed.is_empty() {
                println!(
                    "✅ Completed pages: {:?}",
                    completed.into_iter().collect::<Vec<_>>()
                );
            }
        }
        Err(e) => {
            println!("❌ Failed to get resume info: {:?}", e);
        }
    }

    // Initialize shared state
    let scraping_state = ScrapingState {
        data: Arc::new(Mutex::new(Vec::new())),
        info: Arc::new(Mutex::new(json!({
            "status": "initializing",
            "cpu_cores_used": cpu_cores,
            "website": "https://zzxxtra.com/",
            "database_connected": true,
            "resume_enabled": true
        }))),
        db_pool,
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

    // In-memory data endpoint
    let api_route = warp::path("api")
        .and(warp::path("data"))
        .and(warp::get())
        .and(state_filter.clone())
        .and_then(get_data);

    // PostgreSQL data endpoint with query parameters
    let db_route = warp::path("api")
        .and(warp::path("get"))
        .and(warp::get())
        .and(state_filter.clone())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and_then(get_db_data);

    // Resume info endpoint
    let resume_route = warp::path("api")
        .and(warp::path("resume"))
        .and(warp::get())
        .and(state_filter)
        .and_then(get_resume_info);

    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec!["content-type"])
        .allow_methods(vec!["GET", "POST"]);

    let routes = html_route
        .or(api_route)
        .or(db_route)
        .or(resume_route)
        .with(cors);

    println!("✅ Web server started on http://localhost:3030");
    println!(
        "🔄 Scraping started in background with {} CPU cores and resume functionality",
        cpu_cores
    );
    println!("📊 API endpoints:");
    println!("   - GET /api/data - In-memory data");
    println!("   - GET /api/get?limit=50&offset=0 - PostgreSQL data");
    println!("   - GET /api/resume - Resume information");

    warp::serve(routes).run(([0, 0, 0, 0], 3030)).await;

    Ok(())
}
