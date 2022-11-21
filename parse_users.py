import asyncio
import re
from collections import namedtuple
import asyncpg
from selenium import webdriver
from selenium.common import NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

mail_regex = re.compile(
    r"[^@\s]+@[^@\s]+\.[a-zA-Z0-9]+", re.MULTILINE | re.IGNORECASE
)
url_regex = re.compile(
    "^https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)$",
    re.MULTILINE | re.IGNORECASE
)
phone_regex = re.compile(
    r"(\d{3}[-\.\s]??\d{3}[-\.\s]??\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]??\d{4}|\d{3}[-\.\s]??\d{4})",
    re.MULTILINE | re.IGNORECASE
)
url = 'https://dzen.ru'
Header = namedtuple('Header', ['title', 'email', 'description', 'subscribers', 'subscriptions', 'url', 'phone'])
Post = namedtuple('Post', ['title', 'description', 'date', 'url'])


def parse_header(session: WebDriver) -> Header:
    session.implicitly_wait(5)
    header = session.find_element(by=By.CLASS_NAME, value="desktop-channel-info-layout")
    title = session.find_element(By.CLASS_NAME, "channel-title__block-nt").text
    found_mail = mail_regex.search(header.text)
    found_mail = found_mail.group() if found_mail else None
    found_url = url_regex.search(header.text)
    found_url = found_url.group() if found_url else None
    found_phone = phone_regex.search(header.text)
    found_phone = found_phone.group() if found_phone else None
    description = session.find_element(By.CLASS_NAME, "desktop-channel-info-layout__description").text.replace("\n",
                                                                                                               " ")
    counters = session.find_elements(By.CLASS_NAME, "desktop-channel-info-layout__counter")
    subscribers = counters[0].text.split('\n')[0]
    subscriptions = counters[1].text.split('\n')[0]

    print(
        f'Parsing:\n{title=}\n{found_mail=}\n{description=}\n{subscribers=}\n{subscriptions=}\n{found_url=}\n{found_phone=}\n')
    return Header(title, found_mail, description, subscribers, subscriptions, found_url, found_phone)


def init_webdriver() -> WebDriver:
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    return webdriver.Chrome(options=options)


async def parse_post(post: WebElement) -> Post | None:
    try:
        content = post.find_element(By.CLASS_NAME, "card-image-compact-view__content").text.split('\n')
        u = post.find_element(By.CLASS_NAME, "card-image-compact-view__clickable").get_attribute('href')
        print(f'Parsing post:\n{content=}\n{u=}\n')
        return Post(content[0], content[1], content[2], u)
    except NoSuchElementException:
        print(f'Broken post\n')
        return None


async def parse_user(user_id: dict[str, str], pool: asyncpg.Pool) -> None:
    browser = init_webdriver()
    browser.get(f"{url}/{'id' if user_id['type'] == 'id' else ''}/{user_id['id']}")
    header = parse_header(browser)
    user: int | None = None
    async with pool.acquire() as connection:
        try:
            async with connection.transaction():
                await connection.execute(
                    'INSERT INTO parsed_user (title, description, email, phone, subscribers, subscriptions) VALUES ($1, $2, $3, $4, $5, $6)',
                    header.title, header.description, header.email, header.phone, header.subscribers,
                    header.subscriptions
                )
                user = (await connection.fetch('SELECT id FROM parsed_user WHERE title = $1', header.title))[0]['id']
        except asyncpg.exceptions.UniqueViolationError:
            async with connection.transaction():
                user = (await connection.fetch('SELECT id FROM parsed_user WHERE title = $1', header.title))[0]['id']
                await connection.execute(
                    "DELETE FROM parsed_post WHERE id = $1",
                    user
                )
    if user is None:
        raise Exception("User not found")
    browser.implicitly_wait(5)
    posts: list[WebElement] = browser.find_elements(By.CLASS_NAME, "feed__row")
    last_post_number: int = len(posts)
    while True:
        await asyncio.sleep(1)
        ActionChains(browser).scroll_to_element(posts[-1]).perform()
        new_posts: list[WebElement] = browser.find_elements(By.CLASS_NAME, "feed__row")
        if len(new_posts) == last_post_number:
            break
        last_post_number = len(new_posts)
        posts = new_posts
        print(f"Found new {len(posts)} posts. Scrolling...")
    print(f"Found {len(posts)} posts. Parsing...")
    prepared_posts = await asyncio.gather(*[parse_post(post) for post in
                                            posts])
    pushed_posts = 0
    for post in prepared_posts:
        async with pool.acquire() as connection:
            async with connection.transaction():
                if post is not None:
                    await connection.execute(
                        'INSERT INTO parsed_post (title, description, last, url, author) VALUES ($1, $2, $3, $4, $5)',
                        post.title, post.description, post.date, post.url, user
                    )
                    pushed_posts += 1
    print(f"Pushed {pushed_posts} posts")
    browser.quit()


async def parse(users: list[dict[str, str]], pool: asyncpg.Pool) -> None:
    await asyncio.gather(*[parse_user(user, pool) for user in users])
