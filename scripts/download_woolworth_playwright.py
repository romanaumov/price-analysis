# pip install playwright
# python -m playwright install

import csv
import time
import os
import random
from urllib.parse import urlparse, parse_qs
# from playwright.sync_api import sync_playwright
from playwright.async_api import async_playwright
import asyncio
from itertools import islice
from google.cloud import storage

# List of user agents to rotate
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36'
]

BUCKET_NAME = "ny-taxi-449605-csv-bucket"

async def download_pages(iteration, csv_file, output_folder):
    """Download JavaScript-rendered pages using Playwright."""
    
    # Start timer
    start_time = time.time()  
    
    os.makedirs(output_folder, exist_ok=True)
    success_count = 0
    error_count = 0
    total_count = 0
    skipped_count = 0
    execution_time = 0

    async with async_playwright() as pw:
    # with sync_playwright() as p:
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Launch Chromium browser with configured options
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                '--no-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage'
            ]
        )

        # Create browser context with random user agent
        user_agent = random.choice(USER_AGENTS)
        context = await browser.new_context(user_agent=user_agent)

        # with open(csv_file, 'r', encoding='utf-8') as file:
        #     reader = file.readlines()
            
        #     for url_line in reader[iteration*3:(iteration+1)*3]:
        #         url = url_line.strip()
        #         if not url:
        #             continue
        
        with open(csv_file, 'r', encoding='utf-8') as file:
            # Using islice to read lines in chunks of 3
            for url_line in islice(file, iteration*3, (iteration+1)*3):
                url = url_line.strip()
                if not url:
                    continue
                # Add your logic for using the url here
                
                # Parse URL for filename generation
                parsed_url = urlparse(url)
                query_params = parse_qs(parsed_url.query)
                stockcode = query_params.get('stockcode', [None])[0]
                name = query_params.get('name', [None])[0]

                # Generate filename
                if stockcode and name:
                    filename = f"{stockcode}_{name}.js"
                else:
                    filename = f"page_{success_count}.js"
                filepath = os.path.join(output_folder, filename)

                # **Check if file already exists**
                if os.path.exists(filepath):
                    skipped_count += 1
                    print(f"Skipping {filename}, already exists.")
                    continue  # Skip to the next URL
                
                # Create new page for each request
                page = await context.new_page()
                
                try:
                    # Navigate to page with timeout
                    await page.goto(url, timeout=10000)
                    # Wait for the webpage to fully load
                    await page.wait_for_load_state("networkidle")

                    # Get rendered page content
                    page_source = await page.content()

                    # Save content to file
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(page_source)
                        
                    # Upload to GCS
                    output_blob = bucket.blob(filename)
                    output_blob.upload_from_filename(filepath)

                    success_count += 1
                    total_count += 1
                    
                    end_time = time.time()  # End timer
                    execution_time = end_time - start_time
                    
                    print(f"No: {total_count}, Time: {execution_time:.2f} sec, Downloaded: {filename}")

                except Exception as e:
                    error_count += 1
                    print(f"Error downloading {url}: {str(e)}")

                finally:
                    print(f"Total: {total_count}, Error: {error_count}, Skipped: {skipped_count}, Time: {execution_time:.2f} sec")
                    await page.close()

                # Add random delay between requests
                time.sleep(random.uniform(1, 3))

        # Close browser after processing all URLs
        await browser.close()

    print(f"\nDownload complete!\nSuccess: {success_count}\nErrors: {error_count}\nSkipped: {skipped_count}")
    
if __name__ == "__main__":
    csv_file = "/home/airflow/price-analysis/input/urls.csv"
    output_folder = "/home/airflow/price-analysis/output"
    i = 0 # for testing purpose
    asyncio.run(download_pages(i, csv_file, output_folder))
