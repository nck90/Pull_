import httpx
import asyncio
import json
import os
import base64
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.environ.get('TOKEN')
HEADERS = {'Authorization': f'token {TOKEN}'}
MAX_REPOS = 5000  # Target number of repositories to process
BATCH_SIZE = 100  # Number of repositories to process per batch
CONCURRENCY_LIMIT = 3  # Number of concurrent requests to GitHub API
RATE_LIMIT_WAIT = 60  # Seconds to wait when rate limited

# Directory setup
DATA_DIR = 'github_data'
REPO_DATA_DIR = os.path.join(DATA_DIR, 'repositories')
JSON_OUTPUT_DIR = os.path.join(DATA_DIR, 'json_output')
LOG_FILE = os.path.join(DATA_DIR, 'scraper_log.txt')

# Create necessary directories
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(REPO_DATA_DIR, exist_ok=True)
os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    print(log_entry)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(log_entry + '\n')

async def fetch_with_retry(client, url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = await client.get(url, timeout=30, headers=HEADERS)
            
            # Check for rate limiting
            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
                remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                if remaining == 0:
                    reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                    current_time = time.time()
                    sleep_time = reset_time - current_time + 5  # Add 5 seconds buffer
                    
                    if sleep_time > 0:
                        log_message(f"Rate limit hit. Waiting for {sleep_time:.2f} seconds...")
                        await asyncio.sleep(sleep_time)
                        retries += 1
                        continue
            
            response.raise_for_status()
            return response
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # Not found, no need to retry
                raise
            elif e.response.status_code == 403:
                # Probably rate limited
                log_message(f"Rate limit hit or forbidden. Waiting for {RATE_LIMIT_WAIT} seconds...")
                await asyncio.sleep(RATE_LIMIT_WAIT)
            else:
                log_message(f"HTTP error {e.response.status_code} for {url}: {str(e)}")
                await asyncio.sleep(2)
            retries += 1
        except Exception as e:
            log_message(f"Error fetching {url}: {str(e)}")
            await asyncio.sleep(2)
            retries += 1
    
    raise Exception(f"Failed to fetch {url} after {max_retries} retries")

async def search_repositories(client, query, page, per_page=100):
    url = f"https://api.github.com/search/repositories?q={query}&sort=stars&order=desc&page={page}&per_page={per_page}"
    response = await fetch_with_retry(client, url)
    data = response.json()
    return data.get('items', [])

def filter_test_files(tree_data):
    return [
        f for f in tree_data.get('tree', []) 
        if f.get('type') == 'blob' and 
        ('test' in f.get('path', '').lower() or 'tests' in f.get('path', '').lower()) and 
        f.get('path', '').endswith('.py')
    ]

def filter_python_files(tree_data):
    return [
        f for f in tree_data.get('tree', []) 
        if f.get('type') == 'blob' and 
        f.get('path', '').endswith('.py') and
        'venv' not in f.get('path', '').lower() and
        'env' not in f.get('path', '').lower() and
        'build' not in f.get('path', '').lower() and
        'dist' not in f.get('path', '').lower()
    ]

async def get_file_content(client, content_url):
    try:
        response = await fetch_with_retry(client, content_url)
        content_data = response.json()
        
        if isinstance(content_data, list):
            # This is a directory, not a file
            return None
        
        if content_data.get('encoding') == 'base64':
            content = base64.b64decode(content_data.get('content', '')).decode('utf-8', errors='replace')
            return content
        return None
    except Exception as e:
        log_message(f"Error fetching content from {content_url}: {str(e)}")
        return None

async def process_repo(client, repo, semaphore):
    async with semaphore:
        try:
            owner = repo['owner']['login']
            name = repo['name']
            full_name = f"{owner}/{name}"
            
            log_message(f"Processing repository: {full_name}")
            
            # Check default branch
            default_branch = repo.get('default_branch', 'main')
            
            # Try to get tree data
            tree_url = f"https://api.github.com/repos/{full_name}/git/trees/{default_branch}?recursive=1"
            try:
                tree_response = await fetch_with_retry(client, tree_url)
                tree_data = tree_response.json()
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and default_branch == 'main':
                    # Try with master branch instead
                    tree_url = f"https://api.github.com/repos/{full_name}/git/trees/master?recursive=1"
                    try:
                        tree_response = await fetch_with_retry(client, tree_url)
                        tree_data = tree_response.json()
                    except Exception:
                        log_message(f"Failed to get tree for {full_name}, skipping")
                        return None
                else:
                    log_message(f"Failed to get tree for {full_name}, skipping")
                    return None
            
            # Find test files
            test_files = filter_test_files(tree_data)[:10]  # Limit to 10 test files
            
            # Find Python files if we don't have enough test files
            python_files = []
            if len(test_files) < 5:
                python_files = filter_python_files(tree_data)[:10 - len(test_files)]
            
            all_files = test_files + python_files
            
            if not all_files:
                log_message(f"No suitable Python files found in {full_name}, skipping")
                return None
            
            # Get content of each file
            files_content = {}
            for file_info in all_files:
                file_path = file_info['path']
                content_url = f"https://api.github.com/repos/{full_name}/contents/{file_path}"
                content = await get_file_content(client, content_url)
                
                if content:
                    files_content[file_path] = content
            
            if not files_content:
                log_message(f"Could not fetch any file content from {full_name}, skipping")
                return None
            
            # Get requirements.txt if available
            requirements = None
            for file_info in tree_data.get('tree', []):
                if file_info.get('path') == 'requirements.txt':
                    content_url = f"https://api.github.com/repos/{full_name}/contents/requirements.txt"
                    requirements = await get_file_content(client, content_url)
                    break
            
            # Create JSON output
            repo_data = {
                'repo_name': full_name,
                'repo_url': repo['html_url'],
                'description': repo.get('description', ''),
                'stars': repo.get('stargazers_count', 0),
                'language': repo.get('language', ''),
                'created_at': repo.get('created_at', ''),
                'updated_at': repo.get('updated_at', ''),
                'files': files_content,
                'requirements': requirements
            }
            
            # Save to JSON
            repo_filename = f"{owner}_{name}.json"
            repo_path = os.path.join(JSON_OUTPUT_DIR, repo_filename)
            
            with open(repo_path, 'w', encoding='utf-8') as f:
                json.dump(repo_data, f, ensure_ascii=False, indent=2)
            
            log_message(f"Successfully processed {full_name} with {len(files_content)} files")
            
            return {
                'repo': full_name,
                'files_count': len(files_content),
                'has_requirements': requirements is not None,
                'json_path': repo_path
            }
        
        except Exception as e:
            log_message(f"Error processing repository {repo.get('full_name', 'unknown')}: {str(e)}")
            return None

async def main():
    start_time = time.time()
    log_message(f"Starting large-scale GitHub repository scraper targeting {MAX_REPOS} repositories")
    
    # Load previously processed repos to avoid duplicates
    processed_file = os.path.join(DATA_DIR, 'processed_repos.json')
    processed = set(json.load(open(processed_file, 'r')) if os.path.exists(processed_file) else [])
    
    log_message(f"Found {len(processed)} previously processed repositories")
    
    # Initialize results list
    results = []
    
    # Search queries to use
    search_queries = [
        "language:Python+stars:>100",
        "language:Python+pytest+stars:>10",
        "language:Python+fastapi+stars:>10",
        "language:Python+django+stars:>10",
        "language:Python+flask+stars:>10",
        "language:Python+machine+learning+stars:>10",
        "language:Python+data+science+stars:>10",
        "language:Python+tensorflow+stars:>10",
        "language:Python+pytorch+stars:>10",
        "language:Python+nlp+stars:>10"
    ]
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    async with httpx.AsyncClient(timeout=30) as client:
        query_index = 0
        page = 1
        repos_processed = 0
        
        while repos_processed < MAX_REPOS:
            query = search_queries[query_index]
            
            try:
                log_message(f"Searching for repositories with query: {query} (page {page})")
                repos = await search_repositories(client, query, page)
                
                if not repos:
                    log_message(f"No more repositories found for query: {query}")
                    query_index = (query_index + 1) % len(search_queries)
                    page = 1
                    continue
                
                # Filter out already processed repos
                new_repos = [r for r in repos if r['full_name'] not in processed]
                
                if not new_repos:
                    log_message(f"No new repositories found on page {page}, moving to next page")
                    page += 1
                    if page > 10:  # GitHub API limits to 1000 results (10 pages of 100)
                        query_index = (query_index + 1) % len(search_queries)
                        page = 1
                    continue
                
                log_message(f"Processing batch of {len(new_repos)} repositories")
                
                # Process repos
                tasks = [process_repo(client, repo, semaphore) for repo in new_repos]
                batch_results = [r for r in await asyncio.gather(*tasks) if r]
                
                # Update processed repos
                for repo in new_repos:
                    processed.add(repo['full_name'])
                
                # Save processed repos periodically
                with open(processed_file, 'w') as f:
                    json.dump(list(processed), f)
                
                # Update results
                results.extend(batch_results)
                repos_processed += len(batch_results)
                
                log_message(f"Progress: {repos_processed}/{MAX_REPOS} repositories processed")
                
                # Save results periodically
                with open(os.path.join(DATA_DIR, 'results.json'), 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                
                # Move to next page
                page += 1
                if page > 10:  # GitHub API limits to 1000 results (10 pages of 100)
                    query_index = (query_index + 1) % len(search_queries)
                    page = 1
                
                # Sleep to avoid hitting rate limits
                await asyncio.sleep(2)
            
            except Exception as e:
                log_message(f"Error in main loop: {str(e)}")
                await asyncio.sleep(10)
                continue
    
    # Final save of results
    with open(os.path.join(DATA_DIR, 'final_results.json'), 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    end_time = time.time()
    duration = end_time - start_time
    log_message(f"Scraping completed. Processed {repos_processed} repositories in {duration:.2f} seconds")
    log_message(f"Final results saved to {os.path.join(DATA_DIR, 'final_results.json')}")

if __name__ == "__main__":
    if not TOKEN:
        print("GitHub 토큰을 설정하세요. .env 파일에 TOKEN='your_github_token_here' 추가 또는 환경변수 설정")
    else:
        asyncio.run(main())