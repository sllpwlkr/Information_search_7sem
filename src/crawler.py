import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import time
import logging
import re
from datetime import datetime
import pywikibot
from database import Database
from config import load_config
import requests
import xml.etree.ElementTree as ET
from parser import parse_html

logger = logging.getLogger(__name__)


class WikiCrawler:
    def __init__(self, config):
        self.config = config['logic']
        self.db_config = config['db']

        self.db = Database(config)

        self.delay = self.config['delay_between_requests']
        self.max_pages = self.config['max_pages']
        self.start_category = self.config['start_category']
        self.max_depth = self.config['max_depth']
        self.revisit_interval = self.config.get('revisit_interval_days', 7) * 86400

        self.site = pywikibot.Site("ru", "wikipedia")
        self.start_category_obj = pywikibot.Category(
            self.site,
            f"Категория:{self.start_category}"
        )
        self.min_words = self.config.get('min_words', 1000)

        self.downloaded = 0
        self.updated = 0
        self.skipped = 0
        self.too_short = 0

    def process_page(self, page):
        try:
            url = page.full_url()
            normalized_url = self.db.normalize_url(url)

            existing = self.db.get_document(normalized_url)
            current_time = int(time.time())

            if existing:
                if current_time - existing.get('updated_at', 0) < self.revisit_interval:
                    logger.debug(f"Документ не требует обновления: {page.title()}")
                    self.skipped += 1
                    return False

            logger.info(f"Скачивание: {page.title()}")
            html = page.get_parsed_page()

            clean_text, word_count = parse_html(html, url=url)

            if word_count < self.min_words:
                logger.info(f"Статья слишком короткая ({word_count} слов < {self.min_words}): {page.title()}")
                self.too_short += 1

                if existing:
                    self.db.collection.delete_one({"_id": existing["_id"]})
                    logger.info(f"Удалена существующая короткая статья: {page.title()}")

                return False

            content_hash = self.db.compute_hash(clean_text)

            if existing and existing.get('content_hash') == content_hash:
                self.db.collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"updated_at": current_time}}
                )
                logger.debug(f"Контент не изменился: {page.title()}")
                self.skipped += 1
                return False

            document_data = {
                "url": url,
                "normalized_url": normalized_url,
                "source_name": "Wikipedia",
                "raw_html": html,
                "clean_text": clean_text,
                "word_count": word_count,
                "crawled_at": current_time,
                "updated_at": current_time,
                "content_hash": content_hash,
                "status": "processed",
                "metadata": {
                    "title": page.title(),
                    "language": "ru",
                    "categories": [cat.title() for cat in page.categories()]
                }
            }

            self.db.save_document(document_data)

            if existing:
                self.updated += 1
                logger.info(f"Обновлено: {self.updated} | {page.title()}")
            else:
                self.downloaded += 1
                logger.info(f"Скачано: {self.downloaded} | {page.title()}")

            return True

        except Exception as e:
            logger.error(f"Ошибка при обработке {page.title()}: {e}")
            return False

    def crawl_category(self, category, depth=0):
        if depth > self.max_depth or (self.downloaded + self.updated) >= self.max_pages:
            return

        logger.info(f"Обход категории: {category.title()} (глубина: {depth})")

        try:
            articles = list(category.articles())
            logger.info(f"Найдено статей: {len(articles)}")

            for page in articles:
                if (self.downloaded + self.updated) >= self.max_pages:
                    break

                self.process_page(page)
                time.sleep(self.delay)

            if depth < self.max_depth:
                subcategories = list(category.subcategories())
                logger.info(f"Найдено подкатегорий: {len(subcategories)}")

                for subcat in subcategories:
                    if (self.downloaded + self.updated) < self.max_pages:
                        self.crawl_category(subcat, depth + 1)

        except Exception as e:
            logger.error(f"Ошибка при обходе категории {category.title()}: {e}")

    def continue_crawling(self):
        logger.info("Начало новой сессии обхода")
        self.crawl_category(self.start_category_obj)

    def run(self):
        logger.info("=" * 60)
        logger.info("ЗАПУСК ПОИСКОВОГО РОБОТА")
        logger.info(f"Начальная категория: {self.start_category}")
        logger.info(f"Максимальная глубина: {self.max_depth}")
        logger.info(f"Целевое количество: {self.max_pages}")
        logger.info(f"Минимум слов в статье: {self.min_words}")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            self.continue_crawling()
            elapsed = time.time() - start_time
            stats = self.db.get_statistics()

            logger.info("=" * 60)
            logger.info("ОБРАБОТКА ЗАВЕРШЕНА")
            logger.info(f"Время работы: {elapsed:.2f} секунд")
            logger.info(f"Новых документов: {self.downloaded}")
            logger.info(f"Обновленных документов: {self.updated}")
            logger.info(f"Пропущено: {self.skipped}")
            logger.info(f"Всего в БД: {stats['total_documents']}")
            logger.info("=" * 60)

        except KeyboardInterrupt:
            logger.info("Робот остановлен пользователем")
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
        finally:
            self.db.close()


class BreCrawler:

    def __init__(self, config):
        self.full_config = config
        self.cfg = config.get("bre", {})
        self.db = Database(config)

        self.enabled = bool(self.cfg.get("enabled", True))
        self.sitemap_index = self.cfg.get("sitemap_index", "https://old.bigenc.ru/sitemaps/sitemap.xml")
        self.allowed_prefixes = self.cfg.get("allowed_path_prefixes", ["https://old.bigenc.ru/biology/text/"])

        self.delay = float(self.cfg.get("delay_between_requests", config["logic"]["delay_between_requests"]))
        self.max_pages = int(self.cfg.get("max_pages", 0))  # 0 = без лимита, но лучше задавать
        self.revisit_interval = int(self.cfg.get("revisit_interval_days", 14)) * 86400
        self.min_words = int(self.cfg.get("min_words", config["logic"].get("min_words", 500)))

        self.user_agent = self.cfg.get("user_agent", "SearchBot/1.0")
        self.timeout_sec = int(self.cfg.get("timeout_sec", 20))

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

        self.downloaded = 0
        self.updated = 0
        self.skipped = 0
        self.too_short = 0

    def _fetch(self, url: str) -> str | None:
        try:
            r = self.session.get(url, timeout=self.timeout_sec)
            if r.status_code != 200:
                logger.warning(f"BRE: HTTP {r.status_code} для {url}")
                return None
            r.encoding = r.encoding or "utf-8"
            return r.text
        except Exception as e:
            logger.error(f"BRE: ошибка запроса {url}: {e}")
            return None

    def _parse_sitemap_urls(self, sitemap_xml: str) -> list[str]:
        urls = []
        try:
            root = ET.fromstring(sitemap_xml)
        except Exception as e:
            logger.error(f"BRE: ошибка XML: {e}")
            return urls

        def strip_ns(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        root_tag = strip_ns(root.tag)

        if root_tag == "sitemapindex":
            for sm in root:
                if strip_ns(sm.tag) != "sitemap":
                    continue
                loc = None
                for ch in sm:
                    if strip_ns(ch.tag) == "loc":
                        loc = (ch.text or "").strip()
                        break
                if loc:
                    urls.append(loc)

        elif root_tag == "urlset":
            for u in root:
                if strip_ns(u.tag) != "url":
                    continue
                loc = None
                for ch in u:
                    if strip_ns(ch.tag) == "loc":
                        loc = (ch.text or "").strip()
                        break
                if loc:
                    urls.append(loc)

        else:
            logger.warning(f"BRE: неизвестный тип sitemap: {root_tag}")

        return urls

    def _is_allowed_url(self, url: str) -> bool:
        return any(url.startswith(pfx) for pfx in self.allowed_prefixes)

    def extract_title(self, html: str) -> str:
        try:
            soup = BeautifulSoup(html, "html.parser")

            # 1) og:title (часто самый точный)
            og = soup.find("meta", attrs={"property": "og:title"})
            if og and og.get("content"):
                t = og["content"].strip()
                if t:
                    return t

            # 2) h1
            h1 = soup.find("h1")
            if h1:
                t = h1.get_text(" ", strip=True)
                if t:
                    return t

            if soup.title:
                t = soup.title.get_text(" ", strip=True)
                if t:
                    t = re.sub(r"\s*[-|—]\s*.*$", "", t).strip()
                    if t:
                        return t

            mt = soup.find("meta", attrs={"name": "title"})
            if mt and mt.get("content"):
                t = mt["content"].strip()
                if t:
                    return t

        except Exception:
            pass

        return "Без названия"

    def process_url(self, url: str) -> bool:
        try:
            normalized_url = self.db.normalize_url(url)
            existing = self.db.get_document(normalized_url)
            now = int(time.time())

            if existing:
                if now - existing.get("updated_at", 0) < self.revisit_interval:
                    self.skipped += 1
                    return False

            html = self._fetch(url)
            if not html:
                self.skipped += 1
                return False

            clean_text, word_count = parse_html(html, url=url)

            if word_count < self.min_words:
                logger.info(f"BRE: статья короткая ({word_count} < {self.min_words}): {url}")
                self.too_short += 1
                if existing:
                    self.db.collection.delete_one({"_id": existing["_id"]})
                return False

            content_hash = self.db.compute_hash(clean_text)

            if existing and existing.get("content_hash") == content_hash:
                self.db.collection.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"updated_at": now}}
                )
                self.skipped += 1
                return False

            title = self.extract_title(html)

            doc = {
                "url": url,
                "normalized_url": normalized_url,
                "source_name": "BRE",
                "raw_html": html,
                "clean_text": clean_text,
                "word_count": word_count,
                "crawled_at": now,
                "updated_at": now,
                "content_hash": content_hash,
                "status": "processed",
                "metadata": {
                    "title": title or "Без названия",
                    "language": "ru"
                }
            }

            self.db.save_document(doc)

            if existing:
                self.updated += 1
            else:
                self.downloaded += 1

            logger.info(f"BRE: ok (new={self.downloaded}, upd={self.updated}, skip={self.skipped}) | {title or url}")
            return True

        except Exception as e:
            logger.error(f"BRE: ошибка обработки {url}: {e}")
            self.skipped += 1
            return False

    def run(self):
        if not self.enabled:
            logger.info("BRE: отключено в конфиге (bre.enabled=false)")
            return

        logger.info("=" * 60)
        logger.info("ЗАПУСК ОБХОДА БРЭ (old.bigenc.ru) ПО SITEMAP")
        logger.info(f"Sitemap index: {self.sitemap_index}")
        logger.info(f"Allowed prefixes: {self.allowed_prefixes}")
        logger.info(f"Min words: {self.min_words}, Max pages: {self.max_pages}")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            index_xml = self._fetch(self.sitemap_index)
            if not index_xml:
                logger.error("BRE: не удалось скачать sitemap_index")
                return

            sitemaps = self._parse_sitemap_urls(index_xml)
            looks_like_sitemap_files = any(u.endswith(".xml") for u in sitemaps)

            if looks_like_sitemap_files:
                sitemap_files = sitemaps
                logger.info(f"BRE: найдено sitemap-файлов: {len(sitemap_files)}")
                for sm_url in sitemap_files:
                    if self.max_pages and (self.downloaded + self.updated) >= self.max_pages:
                        break

                    sm_xml = self._fetch(sm_url)
                    if not sm_xml:
                        continue

                    urls = self._parse_sitemap_urls(sm_xml)
                    for url in urls:
                        if self.max_pages and (self.downloaded + self.updated) >= self.max_pages:
                            break
                        if not self._is_allowed_url(url):
                            continue

                        self.process_url(url)
                        time.sleep(self.delay)

            else:
                logger.info(f"BRE: найдено URL статей в sitemap: {len(sitemaps)}")
                for url in sitemaps:
                    if self.max_pages and (self.downloaded + self.updated) >= self.max_pages:
                        break
                    if not self._is_allowed_url(url):
                        continue

                    self.process_url(url)
                    time.sleep(self.delay)

            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info("БРЭ: ОБХОД ЗАВЕРШЕН")
            logger.info(f"Время: {elapsed:.2f} сек")
            logger.info(
                f"Новых: {self.downloaded}, обновлено: {self.updated}, пропущено: {self.skipped}, коротких: {self.too_short}")
            logger.info("=" * 60)

        except KeyboardInterrupt:
            logger.info("BRE: остановлено пользователем")
        finally:
            self.db.close()


def main():
    import sys

    if len(sys.argv) != 2:
        print("Использование: python crawler.py <путь_к_config.yaml>")
        sys.exit(1)

    config_path = sys.argv[1]

    try:
        config = load_config(config_path)

        wiki = WikiCrawler(config)
        wiki.run()

        bre = BreCrawler(config)
        bre.run()


    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()