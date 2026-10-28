# -*- coding: utf-8 -*-
"""
八大党报 电子报爬虫（每日全量，按报纸/日期分类存储）

覆盖报纸（各报官方数字报为准）:
    人民日报   http://paper.people.com.cn/rmrb/
    光明日报   https://epaper.gmw.cn/gmrb/
    中国青年报 http://zqb.cyol.com/
    经济日报   http://paper.ce.cn/
    解放日报   https://www.jfdaily.com/staticsg/res/html/journal/
    新华日报   https://xh.xhby.net/
    解放军报   http://www.81.cn/szb_223187/szblb/
    工人日报   http://www.workercn.cn/papers/grrb/

存储结构:
    数据/八大报纸/人民日报/2026/2026-10-27.csv     # 每报每日一张表
    数据/八大报纸/当日汇总/2026-10-27.csv           # 八报合并表（默认生成，可用 --no-combine 关闭）
    数据/八大报纸/运行日志.txt                      # 1MB 轮转，最多保留 2 份

字段: 报纸 / 发布日期 / 版面序号 / 版面名称 / 标题 / 副标题 / 作者 / 来源 / 摘要 / 正文 / 字数 / 链接 / 采集时间

用法:
    python 八大报纸_爬虫.py                          # 抓今天全部八报，并生成当日汇总
    python 八大报纸_爬虫.py --papers 人民日报,经济日报
    python 八大报纸_爬虫.py --date 2026-10-27
    python 八大报纸_爬虫.py --no-body                # 只抓标题与版面，不抓正文（快）
    python 八大报纸_爬虫.py --no-combine             # 只写分表，不生成当日汇总
    python 八大报纸_爬虫.py --jobs 8                 # 正文并发数
    python 八大报纸_爬虫.py --combine-only --date 2026-10-27   # 仅重建当日汇总表

重复运行幂等：同一链接自动合并/跳过，内容无变化时不重写文件，可安全用于每日计划任务。
"""

import sys

sys.dont_write_bytecode = True  # 不生成 __pycache__，减少无谓磁盘写入

import argparse
import csv
import io
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT_DIR, '数据', '八大报纸')
COMBINE_DIR = os.path.join(OUT_DIR, '当日汇总')
LOG_FILE = os.path.join(OUT_DIR, '运行日志.txt')

FIELDS = ['报纸', '发布日期', '版面序号', '版面名称', '标题', '副标题', '作者',
          '来源', '摘要', '正文', '字数', '链接', '采集时间']

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
}
WRITE_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('八大报纸')
os.makedirs(OUT_DIR, exist_ok=True)
_fh = RotatingFileHandler(LOG_FILE, maxBytes=1_000_000, backupCount=2, encoding='utf-8')
_fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_fh)


# ---------------------------------------------------------------- 基础工具
def fetch(url, retries=3, timeout=30, referer=None):
    headers = dict(HEADERS)
    if referer:
        headers['Referer'] = referer
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                m = re.search(rb'charset=["\']?([\w-]+)', resp.content[:3000])
                enc = m.group(1).decode('ascii', 'ignore').lower() if m else 'utf-8'
                if enc in ('gb2312', 'gbk'):
                    enc = 'gb18030'
                return resp.content.decode(enc, errors='replace')
            logger.debug('HTTP %s: %s', resp.status_code, url)
        except Exception as exc:
            logger.debug('请求失败(%s/%s): %s - %s', attempt + 1, retries, url, exc)
            time.sleep(1.0 * (attempt + 1))
    return None


def fetch_json(url, referer=None):
    text = fetch(url, referer=referer)
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def clean(text):
    if not isinstance(text, str):
        return ''
    text = text.replace('\u3000', ' ').replace('\xa0', ' ')
    return re.sub(r'\s+', ' ', text).strip()


def paragraphs(node):
    if node is None:
        return ''
    lines = [clean(p.get_text(' ', strip=True)) for p in node.select('p')]
    lines = [x for x in lines if x]
    if not lines:
        lines = [clean(x) for x in node.get_text('\n', strip=True).split('\n') if clean(x)]
    return '\n'.join(lines)


def html_to_text(html):
    if not html:
        return ''
    soup = BeautifulSoup(html.replace('<br>', '<br/>').replace('<br />', '<br/>'), 'lxml')
    for br in soup.find_all('br'):
        br.replace_with('\n')
    lines = [clean(x) for x in soup.get_text('\n').split('\n')]
    return '\n'.join([x for x in lines if x])


def ym_of(date):
    return date[:4] + date[5:7]


def dd_of(date):
    return date[8:10]


def ms_of_date(date):
    return datetime.strptime(date, '%Y-%m-%d')


# ---------------------------------------------------------------- 通用 PC 数字报模板
def parse_layout_pages(layout_url, date):
    """解析数字报首页的版面目录（仅当日有效）"""
    html = fetch(layout_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    key = ym_of(date) + '/' + dd_of(date)
    pages = {}
    for a in soup.select('a[href]'):
        href = a['href']
        m = re.search(r'(\d{6})/(\d{2})/node_(\d+)\.html?', href)
        if not m or f'{m.group(1)}/{m.group(2)}' != key:
            continue
        num = int(m.group(3))
        name = re.sub(r'^第\s*\d+\s*版[:：]?\s*', '', clean(a.get_text())).strip()
        pages[num] = {'序号': f'{num:02d}', '名称': name, 'url': urljoin(layout_url, href)}
    return [pages[k] for k in sorted(pages)]


def probe_pages(node_tpl, date, max_pages=32, zero_pad=True, title_re=r'第\s*(\d+)\s*版[:：]\s*([^_|]+)'):
    """非当日回溯：按 node 模板顺序探测版面，连续 3 个缺失即停止"""
    pages, miss = [], 0
    for num in range(1, max_pages + 1):
        url = node_tpl.format(ym=ym_of(date), dd=dd_of(date),
                              n=(f'{num:02d}' if zero_pad else str(num)))
        html = fetch(url)
        if not html:
            miss += 1
            if miss >= 3:
                break
            continue
        miss = 0
        soup = BeautifulSoup(html, 'lxml')
        title = soup.title.get_text(strip=True) if soup.title else ''
        m = re.search(title_re, title)
        name = clean(m.group(2)) if m else ''
        pages.append({'序号': f'{num:02d}', '名称': name, 'url': url})
        time.sleep(0.2)
    return pages


def articles_on_node(node_url, date, content_re=r'/(?:content|con)/\d{6}/\d{2}/content_\d+\.html?'):
    """版面页上的文章链接（按日期过滤，自动去重）"""
    html = fetch(node_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    key = ym_of(date) + '/' + dd_of(date)
    items, seen = [], set()
    for a in soup.select('a[href]'):
        url = urljoin(node_url, a['href'])
        if not re.search(content_re, url):
            continue
        if key not in url:
            continue
        title = clean(a.get_text())
        if not title or title in ('无标题', '下一篇', '上一篇'):
            title = ''
        if url in seen:
            continue
        seen.add(url)
        items.append({'标题': title, '链接': url})
    return items


# ---------------------------------------------------------------- 各报正文解析
def parse_people(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    body = paragraphs(soup.select_one('#articleContent'))
    meta = soup.select_one('.article')
    author, edition, subtitle = '', '', ''
    if meta:
        # p.sec 形如「本报记者 张 洋 《人民日报》（2026年10月27日 第 01 版）」
        sec = meta.select_one('p.sec')
        if sec:
            text = clean(sec.get_text(' '))
            head = text.split('《人民日报》')[0].strip()
            author = head if head and '版' not in head else ''
            m = re.search(r'第\s*(\d+)\s*版', text)
            edition = f'{int(m.group(1)):02d}' if m else ''
        ps = [clean(p.get_text(' ')) for p in meta.select('p')
              if 'sec' not in (p.get('class') or [])]
        ps = [x for x in ps if x and x != author]
        if ps:
            subtitle = ps[1] if len(ps) > 1 else ''
    return {'标题': clean(soup.title.get_text()) if soup.title else '',
            '副标题': subtitle, '作者': author, '来源': '人民日报', '正文': body,
            '版面序号': edition}


def parse_gmrb(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    body = paragraphs(soup.select_one('.m-article-text'))
    info = clean(soup.select_one('.m-article-info').get_text(' ', strip=True)) if soup.select_one('.m-article-info') else ''
    author, edition = '', ''
    if info:
        m = re.search(r'作者[：:]\s*(.+?)\s*《光明日报》', info)
        author = clean(m.group(1)) if m else ''
        m = re.search(r'(\d+)\s*版', info)
        edition = f'{int(m.group(1)):02d}' if m else ''
    title = clean(soup.title.get_text()).split('-光明日报')[0] if soup.title else ''
    return {'标题': title, '作者': author, '来源': '光明日报', '正文': body,
            '版面序号': edition}


def parse_zqb(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    h1 = soup.select_one('h1')
    h2 = soup.select_one('h2')
    body = paragraphs(soup.select_one('#articleContent'))
    author, source, edition, subtitle = '', '中国青年报', '', clean(h2.get_text()) if h2 else ''
    sec = soup.select_one('.article p.sec')
    if sec:
        date_node = sec.select_one('.newstime') or sec.select_one('.date')
        date_text = clean(date_node.get_text()) if date_node else ''
        m = re.search(r'(\d+)\s*版', date_text)
        edition = f'{int(m.group(1)):02d}' if m else ''
        m = re.search(r'来源[：:]\s*([^\s（(]+)', date_text + ' ' + clean(sec.get_text(' ')))
        if m:
            source = m.group(1)
        for span in sec.select('span'):
            if 'date' in (span.get('class') or []) or 'newstime' in (span.get('class') or []):
                continue
            text = clean(span.get_text())
            if text and '来源' not in text and not text.startswith('（'):
                author = text
                break
    if not subtitle:
        for p in soup.select('.article > p'):
            text = clean(p.get_text(' '))
            if text.startswith('——'):
                subtitle = text.lstrip('—').strip()
                break
    return {'标题': clean(h1.get_text()) if h1 else (clean(soup.title.get_text()) if soup.title else ''),
            '副标题': subtitle,
            '作者': author, '来源': source, '正文': body, '版面序号': edition}


def parse_ce(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    node = soup.select_one('.detail-art .content') or soup.select_one('.detail .content')
    body = paragraphs(node)
    return {'标题': clean(soup.title.get_text()) if soup.title else '',
            '来源': '经济日报', '正文': body}


def parse_xhby(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    h3 = soup.select_one('.newsdetatit h3')
    body = paragraphs(soup.select_one('#ozoom')) or paragraphs(soup.select_one('.newsdetatext'))
    edition, source = '', '新华日报'
    author_node = soup.select_one('.newsdetatit .Author')
    if author_node:
        m = re.search(r'(\d+)', clean(author_node.get_text()))
        edition = f'{int(m.group(1)):02d}' if m else ''
    date_node = soup.select_one('.newsdetatit')
    if date_node:
        txt = clean(date_node.get_text(' ', strip=True))
        m = re.search(r'来源[：:]\s*([^\s]+)', txt)
        if m:
            source = m.group(1)
    subtitle = ''
    for p in (soup.select('.newsdetatit p') or []):
        t = clean(p.get_text())
        if t.startswith('——'):
            subtitle = t.lstrip('—').strip()
            break
    # 正文首行常为「□ 新华社记者 XXX」，提取为作者并从正文移除
    author = ''
    lines = body.split('\n')
    if lines and re.match(r'^[□■]\s*', lines[0]):
        author = re.sub(r'^[□■]\s*', '', lines[0]).strip()
        body = '\n'.join(lines[1:])
    title = clean(h3.get_text()) if h3 else ''
    if not title and soup.title:
        title = clean(soup.title.get_text()).split('_新华日报')[0]
    return {'标题': title,
            '副标题': subtitle, '作者': author, '来源': source, '正文': body,
            '版面序号': edition}


def parse_jfdaily(article_id, page_no, date):
    url = (f'https://www.jfdaily.com/staticsg/data/journal/jfrb/{date}/{page_no}/'
           f'article/{article_id}.json')
    data = fetch_json(url, referer='https://www.jfdaily.com/staticsg/res/html/journal/')
    art = (data or {}).get('article') or {}
    return {'标题': clean(art.get('title')) or clean(art.get('introtitle')),
            '副标题': clean(art.get('subtitle')),
            '作者': clean(art.get('author')),
            '来源': clean(art.get('source')) or '解放日报',
            '正文': html_to_text(art.get('content'))}


def parse_grrb(url):
    html = fetch(url)
    if not html:
        return {}
    soup = BeautifulSoup(html, 'lxml')
    title = soup.select_one('#ctitle')
    subtitle = soup.select_one('#subtitle')
    lai = soup.select_one('#lai')
    edition = ''
    if lai:
        m = re.search(r'(\d+)\s*版', clean(lai.get_text()))
        edition = f'{int(m.group(1)):02d}' if m else ''
    return {'标题': clean(title.get_text()) if title else '',
            '副标题': clean(subtitle.get_text()) if subtitle else '',
            '来源': '工人日报',
            '正文': paragraphs(soup.select_one('#ccontent')),
            '版面序号': edition}


# ---------------------------------------------------------------- 各报适配器
def crawl_people(date):
    base = 'http://paper.people.com.cn/rmrb/'
    node_tpl = base + 'pc/layout/{ym}/{dd}/node_{n}.html'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    if not pages:
        pages = probe_pages(node_tpl, date)
    return collect_pc(pages, date, parse_people, '人民日报')


def crawl_gmrb(date):
    base = 'https://epaper.gmw.cn/gmrb/'
    node_tpl = base + 'html/layout/{ym}/{dd}/node_{n}.html'
    pages = parse_layout_pages(base + 'html/layout/index.html', date)
    if not pages:
        pages = probe_pages(node_tpl, date, title_re=r'第\s*(\d+)\s*版[:：]\s*([^_|]+)')
    return collect_pc(pages, date, parse_gmrb, '光明日报')


def crawl_zqb(date):
    base = 'http://zqb.cyol.com/'
    node_tpl = base + 'pc/layout/{ym}/{dd}/node_{n}.html'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    if not pages:
        pages = probe_pages(node_tpl, date)
    return collect_pc(pages, date, parse_zqb, '中国青年报')


def crawl_ce(date):
    base = 'http://paper.ce.cn/'
    node_tpl = base + 'pc/layout/{ym}/{dd}/node_{n}.html'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    if not pages:
        pages = probe_pages(node_tpl, date)
    return collect_pc(pages, date, parse_ce, '经济日报')


def crawl_xhby(date):
    base = 'https://xh.xhby.net/'
    node_tpl = base + 'pc/layout/{ym}/{dd}/node_{n}.html'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    if not pages:
        pages = probe_pages(node_tpl, date, zero_pad=False)
    return collect_pc(pages, date, parse_xhby, '新华日报')


def collect_pc(pages, date, parser, paper):
    """通用流程：遍历版面 -> 取文章链接 -> 抓正文"""
    tasks = []
    for page in pages:
        for art in articles_on_node(page['url'], date):
            tasks.append((page, art))
    logger.info('%s %s 版面 %s 个，待抓正文 %s 条', paper, date, len(pages), len(tasks))
    if not tasks:
        return []
    records = []
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = {}
        for page, art in tasks:
            futures[pool.submit(parser, art['链接'])] = (page, art)
        for fut in as_completed(futures):
            page, art = futures[fut]
            try:
                detail = fut.result() or {}
            except Exception as exc:
                logger.warning('%s 正文解析失败 %s: %s', paper, art['链接'], exc)
                detail = {}
            records.append(build(paper, date, page, art, detail))
    return records


def crawl_jfdaily(date):
    paper = '解放日报'
    navi = fetch_json(f'https://www.jfdaily.com/staticsg/data/journal/jfrb/{date}/navi.json',
                      referer='https://www.jfdaily.com/staticsg/res/html/journal/')
    if not navi or not navi.get('pages'):
        logger.warning('%s %s 版面目录为空（可能尚未出版）', paper, date)
        return []
    tasks = []
    for page in navi['pages']:
        pno = str(page.get('pnumber') or '01')
        for art in page.get('articleList') or []:
            tasks.append(({'序号': f'{int(pno):02d}' if pno.isdigit() else pno,
                           '名称': clean(page.get('pname')) or clean(page.get('pnumber')),
                           'pno': pno},
                          {'链接': f"https://www.jfdaily.com/staticsg/res/html/journal/detail.html"
                                   f"?code=jfrb&date={date}&page={pno}&id={art.get('id')}",
                           'id': art.get('id'),
                           '标题': clean(art.get('title')) or clean(art.get('introtitle')),
                           '作者': clean(art.get('author'))}))
    logger.info('%s %s 版面 %s 个，待抓正文 %s 条', paper, date, len(navi['pages']), len(tasks))

    def work(page, art):
        detail = parse_jfdaily(art['id'], page['pno'], date)
        if not detail.get('标题'):
            detail['标题'] = art.get('标题', '')
        if not detail.get('作者'):
            detail['作者'] = art.get('作者', '')
        return build(paper, date, page, art, detail)

    records = []
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = [pool.submit(work, p, a) for p, a in tasks]
        for fut in as_completed(futures):
            try:
                records.append(fut.result())
            except Exception as exc:
                logger.warning('%s 正文解析失败: %s', paper, exc)
    return records


def crawl_jfjb(date):
    paper = '解放军报'
    y, m, d = date.split('-')
    data = fetch_json(f'http://www.81.cn/_szb/jfjb/{y}/{m}/{d}/index.json')
    if not data or not data.get('paperInfo'):
        logger.warning('%s %s 版面数据为空（可能尚未出版）', paper, date)
        return []
    records = []
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = {}
        for page in data['paperInfo']:
            pno = str(page.get('paperNumber') or '01')
            pinfo = {'序号': f'{int(pno):02d}' if pno.isdigit() else pno,
                     '名称': clean(page.get('paperBk')),
                     'url': page.get('webUrl') or ''}
            for art in page.get('xyList') or []:
                art = {'链接': f"{pinfo['url']}#article-{art.get('id')}",
                       '标题': clean(art.get('title')), '正文': html_to_text(art.get('content'))}
                futures[pool.submit(build, paper, date, pinfo, art,
                                    {'标题': art['标题'], '来源': '解放军报', '正文': art['正文']})] = 1
        for fut in as_completed(futures):
            try:
                records.append(fut.result())
            except Exception as exc:
                logger.warning('%s 正文解析失败: %s', paper, exc)
    logger.info('%s %s 版面 %s 个，正文 %s 条', paper, date, len(data['paperInfo']), len(records))
    return records


def crawl_grrb(date):
    paper = '工人日报'
    y, m, d = date.split('-')
    base = 'http://www.workercn.cn/papers/grrb/{}/{}/{}/'.format(y, m, d)
    tasks, seen, num = [], set(), 1
    while num <= 32:
        page_url = base + f'{num}/page.html'
        html = fetch(page_url)
        if not html:
            break
        soup = BeautifulSoup(html, 'lxml')
        pinfo = {'序号': f'{num:02d}', '名称': f'第{num}版', 'url': page_url}
        for a in soup.select('a[href*="news-"]'):
            url = urljoin(page_url, a['href'])
            if url in seen:
                continue
            seen.add(url)
            tasks.append((pinfo, {'标题': clean(a.get_text()), '链接': url}))
        nxt = None
        for a in soup.select('a'):
            if '下一版' in clean(a.get_text()) and a.get('href') not in ('#', 'javascript:;'):
                nxt = urljoin(page_url, a['href'])
                break
        if not nxt:
            break
        m2 = re.search(r'/(\d+)/page\.html', nxt)
        if not m2:
            break
        num = int(m2.group(1))
    logger.info('%s %s 版面 %s 个，待抓正文 %s 条', paper, date, num, len(tasks))
    records = []
    with ThreadPoolExecutor(max_workers=JOBS) as pool:
        futures = {pool.submit(parse_grrb, art['链接']): (p, art) for p, art in tasks}
        for fut in as_completed(futures):
            page, art = futures[fut]
            try:
                detail = fut.result() or {}
            except Exception as exc:
                logger.warning('%s 正文解析失败 %s: %s', paper, art['链接'], exc)
                detail = {}
            records.append(build(paper, date, page, art, detail))
    return records


def build(paper, date, page, art, detail):
    body = clean_body(detail.get('正文') or '')
    title = clean(detail.get('标题')) or art.get('标题', '')
    return {
        '报纸': paper,
        '发布日期': date,
        '版面序号': detail.get('版面序号') or page.get('序号', ''),
        '版面名称': page.get('名称', ''),
        '标题': title,
        '副标题': clean(detail.get('副标题')),
        '作者': clean(detail.get('作者')),
        '来源': clean(detail.get('来源')) or paper,
        '摘要': body[:150],
        '正文': body,
        '字数': len(body),
        '链接': art.get('链接', ''),
        '采集时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }


def clean_body(body):
    lines = [re.sub(r'^[\s\u3000]+', '', x).strip() for x in body.split('\n')]
    return '\n'.join([x for x in lines if x])


# ---------------------------------------------------------------- 存储
def is_placeholder(rec):
    """过滤版面责编、导读、空标题且空正文的占位条目"""
    title = rec.get('标题') or ''
    if not title and not rec.get('正文'):
        return True
    return bool(re.match(r'^([一两二三四五六七八九十]+版|本版|责任)?责编|^责任编辑|^版式', title)) \
        or title in ('导读', '广告')


def save_records(paper, date, records):
    records = [r for r in records if not is_placeholder(r)]
    if not records:
        return 0
    folder = os.path.join(OUT_DIR, paper, date[:4])
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f'{date}.csv')
    with WRITE_LOCK:
        rows = {}
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8-sig', newline='') as f:
                rows = {row['链接']: row for row in csv.DictReader(f)}
        for rec in records:
            key = rec['链接']
            if rec.get('正文'):
                # 正文抓取成功：以本次结果为准（避免旧的错漏字段残留）
                new = {k: str(rec.get(k) if rec.get(k) is not None else '') for k in FIELDS}
                old = rows.get(key)
                # 内容未变则保留旧记录（含采集时间），避免整表重写
                if old and all((old.get(k) or '') == new[k] for k in FIELDS if k != '采集时间'):
                    new = old
                rows[key] = new
            else:
                old = rows.get(key, {})
                rows[key] = {k: rec.get(k) or old.get(k, '') for k in FIELDS}
        rows = {k: v for k, v in rows.items() if not is_placeholder(v)}
        ordered = sorted(rows.values(), key=lambda r: (r.get('版面序号') or '', r.get('标题') or ''))
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(ordered)
        text = buf.getvalue()
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8-sig', newline='') as f:
                if f.read() == text:
                    return len(rows)  # 无变化不落盘
        with open(path, 'w', encoding='utf-8-sig', newline='') as f:
            f.write(text)
    return len(rows)


def combine_day(date):
    """把当日各报 CSV 合并为一张表"""
    rows = []
    if os.path.isdir(OUT_DIR):
        for paper in PAPERS:
            path = os.path.join(OUT_DIR, paper, date[:4], f'{date}.csv')
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8-sig', newline='') as f:
                    rows.extend(list(csv.DictReader(f)))
    os.makedirs(COMBINE_DIR, exist_ok=True)
    out = os.path.join(COMBINE_DIR, f'{date}.csv')
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    text = buf.getvalue()
    if os.path.exists(out):
        with open(out, 'r', encoding='utf-8-sig', newline='') as f:
            if f.read() == text:
                return len(rows), out
    with open(out, 'w', encoding='utf-8-sig', newline='') as f:
        f.write(text)
    return len(rows), out


# ---------------------------------------------------------------- 主流程
CRAWLERS = {
    '人民日报': crawl_people,
    '光明日报': crawl_gmrb,
    '中国青年报': crawl_zqb,
    '经济日报': crawl_ce,
    '解放日报': crawl_jfdaily,
    '新华日报': crawl_xhby,
    '解放军报': crawl_jfjb,
    '工人日报': crawl_grrb,
}
PAPERS = list(CRAWLERS)

JOBS = 6
FETCH_BODY = True


def run(date, papers, combine=False):
    logger.info('===== 开始抓取 %s：%s =====', date, '、'.join(papers))
    started = datetime.now()
    result = {}
    for paper in papers:
        if not FETCH_BODY:
            # 只抓版面与标题时走各报简化流程：仍调用适配器，但正文置空
            recs = run_titles_only(paper, date)
        else:
            try:
                recs = CRAWLERS[paper](date)
            except Exception as exc:
                logger.exception('%s 抓取失败: %s', paper, exc)
                recs = []
        total = save_records(paper, date, recs)
        result[paper] = (len(recs), total)
        logger.info('%s 完成：新增 %s 条，当日累计 %s 条', paper, len(recs), total)
    count = sum(total for _, total in result.values())
    if combine:
        count, out = combine_day(date)
        logger.info('当日汇总 %s 条 -> %s', count, out)
    cost = (datetime.now() - started).total_seconds()
    logger.info('全部完成：%.1f 秒', cost)
    return result, count


def run_titles_only(paper, date):
    """--no-body：仅标题（不访问正文页）"""
    funcs = {
        '人民日报': lambda d: titles_from_pc(crawl_people_pages(d), d, '人民日报'),
        '光明日报': lambda d: titles_from_pc(crawl_gmrb_pages(d), d, '光明日报'),
        '中国青年报': lambda d: titles_from_pc(crawl_zqb_pages(d), d, '中国青年报'),
        '经济日报': lambda d: titles_from_pc(crawl_ce_pages(d), d, '经济日报'),
        '新华日报': lambda d: titles_from_pc(crawl_xhby_pages(d), d, '新华日报'),
    }
    if paper in funcs:
        return funcs[paper](date)
    recs = CRAWLERS[paper](date)
    for rec in recs:
        rec['正文'], rec['摘要'], rec['字数'] = '', '', 0
    return recs


def crawl_people_pages(date):
    base = 'http://paper.people.com.cn/rmrb/'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    return pages or probe_pages(base + 'pc/layout/{ym}/{dd}/node_{n}.html', date)


def crawl_gmrb_pages(date):
    base = 'https://epaper.gmw.cn/gmrb/'
    pages = parse_layout_pages(base + 'html/layout/index.html', date)
    return pages or probe_pages(base + 'html/layout/{ym}/{dd}/node_{n}.html', date)


def crawl_zqb_pages(date):
    base = 'http://zqb.cyol.com/'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    return pages or probe_pages(base + 'pc/layout/{ym}/{dd}/node_{n}.html', date)


def crawl_ce_pages(date):
    base = 'http://paper.ce.cn/'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    return pages or probe_pages(base + 'pc/layout/{ym}/{dd}/node_{n}.html', date)


def crawl_xhby_pages(date):
    base = 'https://xh.xhby.net/'
    pages = parse_layout_pages(base + 'pc/layout/index.html', date)
    return pages or probe_pages(base + 'pc/layout/{ym}/{dd}/node_{n}.html', date, zero_pad=False)


def titles_from_pc(pages, date, paper):
    records = []
    for page in pages:
        for art in articles_on_node(page['url'], date):
            records.append(build(paper, date, page, art, {'正文': ''}))
    return records


def main():
    global JOBS, FETCH_BODY
    parser = argparse.ArgumentParser(description='八大党报电子报爬虫（每日全量）')
    parser.add_argument('--date', default=datetime.now().strftime('%Y-%m-%d'), help='抓取日期 YYYY-MM-DD')
    parser.add_argument('--papers', default='', help='逗号分隔的报纸名，默认全部')
    parser.add_argument('--jobs', type=int, default=6, help='正文并发数')
    parser.add_argument('--no-body', action='store_true', help='不抓正文，仅标题与版面')
    parser.add_argument('--combine', dest='combine', action='store_true', default=True, help='生成当日八报合并表（默认开启）')
    parser.add_argument('--no-combine', dest='combine', action='store_false', help='不生成当日合并表')
    parser.add_argument('--combine-only', action='store_true', help='仅重建当日汇总表')
    args = parser.parse_args()

    JOBS = max(1, args.jobs)
    FETCH_BODY = not args.no_body
    date = args.date
    try:
        datetime.strptime(date, '%Y-%m-%d')
    except ValueError:
        parser.error('--date 需为 YYYY-MM-DD')

    if args.combine_only:
        count, out = combine_day(date)
        print(f'当日汇总 {count} 条 -> {out}')
        return

    papers = [p.strip() for p in args.papers.split(',') if p.strip()] or PAPERS
    unknown = [p for p in papers if p not in CRAWLERS]
    if unknown:
        parser.error('未知报纸: ' + '、'.join(unknown))

    result, count = run(date, papers, combine=args.combine)

    print('\n' + '=' * 46)
    print(f'{date} 八大报纸抓取结果')
    print('=' * 46)
    for paper, (new, total) in result.items():
        print(f'{paper:<8} 本次 {new:>4} 条   当日累计 {total:>4} 条')
    print('-' * 46)
    print(f'当日合计 {count} 条（按报纸分表存储）')
    if args.combine:
        print(f'合并总表：{os.path.join(COMBINE_DIR, date + ".csv")}')


if __name__ == '__main__':
    main()
