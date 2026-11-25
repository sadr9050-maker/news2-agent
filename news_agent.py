#!/usr/bin/env python3
"""
ایجنت خبری بازار → کانال تلگرام (فارسی)
------------------------------------------------------------
* خبرهای مهم دربارهٔ طلا، نقره، داو جونز، سهام داو، نزدک، بیت‌کوین، یورو، دلار (و پوند، اخبار فد/تورم) را
  از FXStreet، Investing.com، Investopedia، Reuters، CNBC، Financial Times، BLS و Bloomberg می‌خواند، به فارسی ترجمه می‌کند و در کانال تلگرام می‌گذارد.
* تیتر + لینک منتشر می‌شود؛ برای منابع غیرمحدود چکیدهٔ کوتاه فید هم می‌آید. برای Reuters/FT/Bloomberg پیش‌فرض فقط
  تیتر + لینک است (summary_restricted_sources). متن کامل مقاله هرگز کپی نمی‌شود.
* هیچ پکیجی لازم نیست؛ فقط پایتون ۳.۹+.

راه‌اندازی (کاملاً مرحله‌به‌مرحله):
  1) python news_agent.py --setup      ← جادوگر راه‌اندازی: توکن و مقصد را می‌پرسد، تست می‌فرستد و ذخیره می‌کند.
  2) python news_agent.py --dry-run    ← پیش‌نمایش در ترمینال (به تلگرام نمی‌فرستد)
  3) python news_agent.py --backfill 3 ← اجرا و ارسال همان لحظهٔ ۳ خبر اخیر
  4) python news_agent.py              ← اجرای دائمی
  5) python news_agent.py --once       ← فقط یک دور بررسی (برای cron؛ داخل cron هر دقیقه/۲ دقیقه)
  6) python news_agent.py --selftest   ← تست آفلاین (بدون شبکه)
  7) python news_agent.py --test-ai    ← تست کلید Anthropic و نام مدل

⚠ توکن را هرگز جایی منتشر نکن. اگر لو رفت، در BotFather با /revoke عوضش کن.

────────────────────────────────────────────────────────────
چه چیزی در این نسخه نسبت به نسخهٔ قبل بهتر شده (خلاصه):
  • حذف تکراری «جهت‌شناس» شد: «Dollar rises…» و «Dollar falls…» دیگر یکی حساب نمی‌شوند.
  • در شلوغی بازار، خبرهای «مهم» اول صف می‌روند و پنجرهٔ سنیِ بلندتری دارند (high_max_age_minutes).
  • خطای ارسال سقف دارد (۵ تلاش)؛ دیگر یک خبر مشکل‌دار تا ابد صف را نمی‌بندد.
  • استیت کامل ذخیره می‌شود (seen + تیترهای اخیر) و با ری‌استارت، حذف تکراری فازی هم برمی‌گردد؛
    فایل استیت ورژن‌دار و سازگار با فرمت قدیمی است، و با JSON خراب، بی‌صدا صفر نمی‌شود.
  • حافظهٔ `seen` هرس می‌شود؛ رشد نامحدود در پروسهٔ دائمی ندارد.
  • قفل فایل (flock): دو نمونهٔ هم‌زمان پست تکراری نمی‌فرستند. + مدیریت SIGTERM (systemd/docker).
  • لاگ فایل با چرخش + حالت --once برای cron.
  • خواندن متن مقاله برای کل بچ به‌صورت هم‌زمان (latency از ۸×۱۲s به ~۱۲s).
  • باگ‌های ترجمه رفع شد: `_tr_google2` دیگر فقط قطعهٔ اول را برنمی‌گرداند، طول متن برای URL سقف دارد.
  • نام مدل پیش‌فرض Anthropic اصلاح شد و مقدار نامعتبر قبلی به‌صورت خودکار مهاجرت می‌کند. + --test-ai
  • ساعت خبرها به وقت تهران (display_tz) و بررسی بدنهٔ پاسخ تلگرام (ok:false دیگر «موفق» حساب نمی‌شود).
  • فیدهای Google News قابل خاموش کردن‌اند (use_google_news) چون از نظر شرایط استفاده پرریسک‌اند.
  • Yahoo Finance هم به منابع اضافه شد (طلا، نقره، نفت، داو، نزدک، S&P، بیت‌کوین، دلار، پوند، دلار کانادا).
  • تحلیل روزانه با Claude در کد هست ولی پیش‌فرض خاموش است (analysis_enabled)؛ برای روشن کردنش کلید Anthropic لازم است.
"""
import argparse, getpass, hashlib, html, json, os, re, signal, sys, tempfile, time
import urllib.error, urllib.parse, urllib.request
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BASE = os.path.dirname(os.path.abspath(__file__))
CFG_FILE = os.path.join(BASE, "news_config.json")
SEEN_FILE = os.path.join(BASE, "news_seen.json")
LOCK_FILE = os.path.join(BASE, ".news_agent.lock")

STATE_VERSION = 2
SEEN_KEEP = 6000        # سقف کلیدهای نگهداری‌شده (هم در حافظه و هم در فایل)
RECENT_KEEP = 300       # سقف تیترهای اخیر برای حذف تکراری فازی (در فایل ذخیره می‌شود)
SEND_MAX_FAILS = 5      # بعد از این تعداد خطای ارسال، خبر کنار گذاشته می‌شود

DEFAULTS = {
    "token": "",                 # توکن ربات تلگرام
    "channel": "",               # مثل @mychannel یا -100123456789
    "discord_webhook": "",       # اگر پر باشد، پیام‌ها به دیسکورد می‌روند (به‌جای تلگرام)
    "poll_seconds": 60,          # هر چند ثانیه فیدها چک شود
    "max_per_cycle": 8,          # حداکثر پیام در هر دور (برای جلوگیری از اسپم)
    "max_age_minutes": 20,       # فقط خبرهای همین چند دقیقهٔ اخیر منتشر می‌شوند (خبر بدون تاریخ منتشر نمی‌شود)
    "high_max_age_minutes": 90,  # خبرهای «مهم» پنجرهٔ بلندتری دارند تا در شلوغی بازار از دست نروند
    "similar_threshold": 0.5,    # شباهت تیتر برای حذف خبر تکراری (۰ تا ۱)؛ کمتر = سخت‌گیرتر در حذف تکراری
    "translate": True,           # ترجمهٔ فارسی
    "show_original": False,      # True = تیتر انگلیسی هم بیاید (پیش‌فرض: فقط فارسی)
    "include_dow_stocks": True,  # خبر شرکت‌های داو (اپل، مایکروسافت، انویدیا، ...)
    "include_summary": True,     # چکیدهٔ خبر (همان خلاصه‌ای که فید سایت می‌دهد) هم ترجمه و منتشر شود
    "summary_chars": 350,        # حداکثر طول چکیده (حرف)
    "fetch_article": True,       # اگر فید چکیده نداشت، ابتدای متن خبر را از صفحهٔ خبر می‌خواند (رایگان)
    "anthropic_key": "",         # اختیاری: با کلید API از Anthropic، چکیدهٔ فارسی هوشمند و کوتاه (۱ تا ۲ جمله) ساخته می‌شود
    "ai_model": "claude-haiku-4-5",
    "only_high": False,          # True = فقط خبرهای «مهم» (فد، تورم، شوک‌های بازار...) منتشر شود
    "summary_restricted_sources": False,  # True = برای Reuters/FT/Bloomberg هم چکیده منتشر شود (پیش‌فرض: فقط تیتر+لینک)
    "backfill_max_age_minutes": 360,      # پنجرهٔ سن خبر فقط برای --backfill
    "fetch_workers": 8,          # تعداد فیدهای هم‌زمان
    "article_workers": 6,        # تعداد صفحه‌های خبری که هم‌زمان خوانده می‌شوند
    "use_google_news": True,     # فیدهای news.google.com (تنها راه Reuters/FT بدون API) — خاموش کردنش مجازتر است
    "display_tz": "Asia/Tehran", # ساعت خبر در پیام؛ "" یا نامعتبر = ساعت سیستم
    "log_to_file": True,         # لاگ در news_agent.log با چرخش
    "log_max_bytes": 1000000,
    "extra_feeds": [],           # فید دلخواه: [["اسم", "آدرس"], ...]
    # --- تحلیل فاندامنتال روزانه (نیاز به کلید Anthropic) ---
    "analysis_enabled": False,                # True = تحلیل روزانه با Claude (پولی، نیاز به کلید Anthropic). پیش‌فرض: خاموش
    "analysis_times": ["10:00", "15:00"],     # ساعت‌های ارسال (به وقت analysis_tz)
    "analysis_tz": "Asia/Tehran",
    "analysis_grace_minutes": 90,             # اگر اجرای زمان‌بندی‌شده دیر شروع شد، تا این‌قدر بعد از ساعت مقرر هنوز ارسال می‌شود
    "analysis_model": "",                     # خالی = همان ai_model؛ برای کیفیت بیشتر می‌توانی مدل قوی‌تری بگذاری
    "analysis_web_search": False,             # True = Claude قیمت‌ها و خبرهای روز را با جستجوی وب هم می‌خواند (گران‌تر)
    "analysis_web_searches": 6,               # سقف تعداد جستجو در هر تحلیل (فقط وقتی analysis_web_search روشن است)
    "analysis_headlines_per_asset": 8,        # تعداد تیتر ۲۴ ساعت اخیر برای هر دارایی
    "ai_news_summary": True,                  # False = کلید Anthropic فقط برای تحلیل روزانه باشد و برای چکیدهٔ تک‌تک خبرها خرج نشود
}

OLD_AI_MODELS = {"", "claude-haiku-5-5"}     # مقدار نامعتبر نسخهٔ قبل

# منابع: FXStreet + Investing.com + Investopedia + Reuters + CNBC + Financial Times + BLS + Bloomberg
# اگر فیدی کار نکرد خودکار رد می‌شود (فقط یک بار هشدار می‌دهد) و بقیه ادامه می‌دهند.
FEEDS = [
    # --- FXStreet ---
    ("FXStreet",              "https://www.fxstreet.com/rss/news"),
    ("FXStreet Analysis",     "https://www.fxstreet.com/rss/analysis"),
    ("FXStreet",              "https://www.fxstreet.com/google-sitemap-news.xml"),   # پشتیبان (نقشهٔ خبری سایت)
    # --- Investing.com ---
    ("Investing.com",         "https://www.investing.com/rss/news.rss"),
    ("Investing.com Stocks",  "https://www.investing.com/rss/news_25.rss"),
    ("Investing.com Forex",   "https://www.investing.com/rss/news_1.rss"),
    ("Investing.com Commodities", "https://www.investing.com/rss/news_11.rss"),
    ("Investing.com Crypto",  "https://www.investing.com/rss/news_301.rss"),
    ("Investing.com Economy", "https://www.investing.com/rss/news_14.rss"),
    ("Investing.com Indicators", "https://www.investing.com/rss/news_95.rss"),
    # --- Investopedia (فید رسمی‌اش مدت‌هاست خراب است؛ اگر کار نکرد به extra_feeds نگاه کن) ---
    ("Investopedia",          "https://www.investopedia.com/feedbuilder/feed/getfeed?feedName=rss_headline"),
    # --- Reuters (فید RSS رسمی ندارد؛ از فید جستجوی Google News فقط برای reuters.com استفاده می‌شود) ---
    ("Reuters",              "https://news.google.com/rss/search?q=when:1d+site:reuters.com/markets&ceid=US:en&hl=en-US&gl=US"),
    ("Reuters Business",     "https://news.google.com/rss/search?q=when:1d+site:reuters.com/business&ceid=US:en&hl=en-US&gl=US"),
    ("Reuters World",        "https://news.google.com/rss/search?q=when:1d+site:reuters.com/world&ceid=US:en&hl=en-US&gl=US"),
    # --- CNBC ---
    ("CNBC",                 "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114"),   # Top News
    ("CNBC Finance",         "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664"),
    ("CNBC Economy",         "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258"),
    ("CNBC Investing",       "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069"),
    ("CNBC World",           "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362"),
    # --- Financial Times ---
    ("Financial Times",      "https://www.ft.com/rss/home"),
    ("Financial Times Markets", "https://www.ft.com/markets?format=rss"),
    ("Financial Times Economy", "https://www.ft.com/global-economy?format=rss"),
    # --- اوراق قرضهٔ آمریکا (جستجوی Google News؛ ناشر واقعی از تیتر جدا و به‌عنوان منبع نوشته می‌شود) ---
    ("Google News Bonds",     "https://news.google.com/rss/search?q=when:1d+%28%22treasury+yields%22+OR+%22bond+market%22+OR+%22treasury+auction%22+OR+%2210-year+yield%22%29&ceid=US:en&hl=en-US&gl=US"),
    ("Google News Treasuries", "https://news.google.com/rss/search?q=when:1d+%28%22Treasuries%22+OR+%22T-bill%22+OR+%22yield+curve%22+OR+%22Treasury+refunding%22%29&ceid=US:en&hl=en-US&gl=US"),
    # --- BLS (آمار رسمی کار آمریکا: CPI، اشتغال/NFP، PPI) ---
    ("BLS",                  "https://www.bls.gov/feed/bls_latest.rss"),
    ("BLS CPI",              "https://www.bls.gov/feed/cpi.rss"),
    ("BLS Jobs",             "https://www.bls.gov/feed/empsit.rss"),
    ("BLS PPI",              "https://www.bls.gov/feed/ppi.rss"),
    # --- Yahoo Finance (فیدهای خبری شاخص‌ها و دارایی‌ها؛ فید عمومی rssindex از اکتبر ۲۰۲۶ خراب است) ---
    ("Yahoo Finance",    "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GC=F&region=US&lang=en-US"),
    ("Yahoo Finance",  "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SI=F&region=US&lang=en-US"),
    ("Yahoo Finance",     "https://feeds.finance.yahoo.com/rss/2.0/headline?s=CL=F&region=US&lang=en-US"),
    ("Yahoo Finance",     "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EDJI&region=US&lang=en-US"),
    ("Yahoo Finance",  "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EIXIC&region=US&lang=en-US"),
    ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US"),
    ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=BTC-USD&region=US&lang=en-US"),
    ("Yahoo Finance",  "https://feeds.finance.yahoo.com/rss/2.0/headline?s=DX-Y.NYB&region=US&lang=en-US"),
    ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GBPUSD=X&region=US&lang=en-US"),
    ("Yahoo Finance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=USDCAD=X&region=US&lang=en-US"),
    ("Yahoo Finance", "https://news.google.com/rss/search?q=when:1d+site:finance.yahoo.com/economy&hl=en-US&gl=US&ceid=US:en"),
    # --- Forex Factory (فید رسمی ندارد؛ از جستجوی Google News استفاده می‌شود، فقط تیتر + لینک) ---
    ("Forex Factory",        "https://news.google.com/rss/search?q=when:1d+site:forexfactory.com&ceid=US:en&hl=en-US&gl=US"),
    # --- Bloomberg ---
    ("Bloomberg",            "https://feeds.bloomberg.com/markets/news.rss"),
    ("Bloomberg Economics",  "https://feeds.bloomberg.com/economics/news.rss"),
    ("Bloomberg Politics",   "https://feeds.bloomberg.com/politics/news.rss"),
    ("Bloomberg Technology", "https://feeds.bloomberg.com/technology/news.rss"),
]

# منابعی که متن صفحه‌شان را نمی‌خوانیم (paywall / ریدایرکت Google News)؛ فقط تیتر + چکیدهٔ خود فید
NOFETCH_PREFIX = ("Reuters", "Financial Times", "Bloomberg")
# فیدهای BLS مخصوص CPI / اشتغال / PPI همیشه «اقتصاد کلان» و «مهم» حساب می‌شوند
FORCE_MACRO_HIGH = ("BLS CPI", "BLS Jobs", "BLS PPI")

DOW30 = ["apple", "amgen", "amazon", "american express", "boeing", "caterpillar", "salesforce", "cisco", "chevron",
         "disney", "goldman sachs", "home depot", "honeywell", "ibm", "johnson & johnson", "jpmorgan", "coca-cola",
         "mcdonald's", "3m", "merck", "microsoft", "nike", "nvidia", "procter & gamble", "sherwin-williams",
         "travelers", "unitedhealth", "visa", "verizon", "walmart"]

# دسته → (ایموجی، برچسب فارسی، هشتگ، عبارت‌ها)
CATS = {
    "gold":   ("🟡", "طلا",        "#طلا",       ["gold", "bullion", "xau", "precious metals"]),
    "silver": ("⚪", "نقره",       "#نقره",      ["silver", "xag"]),
    "dow":    ("📊", "داو جونز",   "#داو_جونز",  ["dow jones", "dow", "djia", "wall street", "s&p 500", "stocks", "stock market"]),
    "dowstk": ("🏢", "سهام داو",   "#سهام_داو",  DOW30),
    "nasdaq": ("💻", "نزدک",       "#نزدک",      ["nasdaq", "tech stocks", "semiconductor", "semiconductors", "chip stocks", "ai stocks"]),
    "btc":    ("🟠", "بیت‌کوین",   "#بیت_کوین",  ["bitcoin", "btc", "crypto", "cryptocurrency", "cryptocurrencies", "ethereum", "crypto etf",
                                                  "spot bitcoin", "stablecoin"]),
    "eur":    ("💶", "یورو",       "#یورو",      ["euro", "eur/usd", "eurusd", "ecb", "eurozone", "lagarde"]),
    "gbp":    ("💷", "پوند",       "#پوند",      ["pound sterling", "british pound", "uk pound", "the pound", "sterling", "gbp", "gbp/usd", "gbpusd", "bank of england", "boe"]),
    "usd":    ("💵", "دلار",       "#دلار",      ["dollar", "usd", "dxy", "greenback"]),
    "macro":  ("🏦", "اقتصاد کلان", "#اقتصاد",   ["fed", "fomc", "powell", "cpi", "inflation", "payrolls", "nonfarm", "jobs report",
                                                  "rate hike", "rate cut", "interest rates", "tariff", "tariffs", "recession",
                                                  "consumer price index", "producer price index", "employment situation",
                                                  "unemployment rate", "jolts", "pce", "gdp"]),
    "bond":   ("🏛️", "اوراق قرضه", "#اوراق_قرضه", ["treasury", "treasuries", "treasury yield", "treasury yields", "bond yield", "bond yields",
                                                  "bond market", "bond", "bonds", "10-year", "10-year yield", "2-year", "30-year",
                                                  "yield curve", "t-bill", "t-bills", "t-note", "t-bonds",
                                                  "treasury auction", "treasury auctions", "treasury refunding", "10-year note",
                                                  "10-year treasury", "2-year treasury", "30-year treasury", "term premium",
                                                  "bond rally", "bond selloff", "bond sell-off", "bond vigilantes", "debt ceiling"]),
    "vix":    ("😱", "شاخص ترس (VIX)", "#VIX", ["vix", "volatility index", "fear gauge", "fear index", "cboe volatility", "volatility spike",
                                                  "volatility spikes"]),
    "crash":  ("📉", "ریزش ناگهانی", "#ریزش_بازار", ["crash", "crashes", "plunge", "plunges", "plunged", "selloff", "sell-off", "rout",
                                                  "meltdown", "flash crash", "circuit breaker", "circuit breakers", "panic", "bloodbath",
                                                  "trading halt", "tumbles", "tumbled", "slump", "slumps", "wipes out"]),
    "oil":    ("🛢️", "نفت",        "#نفت",        ["oil", "crude", "wti", "brent", "opec", "opec+", "oil prices", "petroleum", "energy prices"]),
    "war":    ("⚔️", "جنگ و ژئوپلیتیک", "#ژئوپلیتیک", ["iran", "iranian", "tehran", "hormuz", "strait of hormuz", "sanctions",
                                                  "war", "ceasefire", "missile", "missiles", "airstrike", "airstrikes", "military",
                                                  "middle east", "israel", "geopolitical", "geopolitics", "escalation", "pentagon"]),
    # چهره‌های اثرگذار بر بازار؛ نام کامل می‌آید تا خبرهای نامرتبط وارد نشوند
    # («trump» عمداً نیامده چون تقریباً همهٔ خبرهای سیاسی را وارد می‌کند؛ اگر خواستی خودت اضافه کن)
    "people": ("👤", "چهره‌های بازار", "#چهره_بازار", ["jensen huang", "elon musk", "tim cook", "satya nadella", "sam altman",
                                                  "warren buffett", "jamie dimon", "larry fink", "scott bessent", "bessent",
                                                  "lisa su", "mark zuckerberg", "sundar pichai", "cathie wood"]),
}
# واژه‌های «عام» که با «مهم» اشتباه می‌شدند حذف شدند: attack / default
HIGH = ["treasury auction", "treasury refunding", "term premium", "debt ceiling", "bond selloff", "bond sell-off",
        "consumer price index", "employment situation", "unemployment rate", "pce", "fed", "fomc", "powell", "cpi", "payrolls", "nonfarm", "rate hike", "rate cut", "rate decision", "lagarde",
        "ecb", "tariff", "tariffs", "iran", "hormuz", "opec", "recession", "crash", "plunge", "plunges", "soar", "soars",
        "tumble", "tumbles", "surge", "surges", "record high", "emergency", "shutdown",
        "war", "ceasefire", "airstrike", "airstrikes", "missile", "missiles", "escalation", "sanctions",
        "vix", "selloff", "sell-off", "rout", "meltdown", "flash crash", "circuit breaker", "circuit breakers", "panic",
        "trading halt", "bloodbath", "yield curve", "10-year yield"]


def _rx(words):
    return re.compile(r"(?<![\w])(?:%s)(?![\w])" % "|".join(re.escape(w) for w in words), re.I)


MARKET_RX = _rx(["market", "markets", "stocks", "stock", "wall street", "futures", "index", "indices", "s&p", "s&p 500",
                 "nasdaq", "dow", "bitcoin", "crypto", "gold", "silver", "dollar", "euro", "yen", "oil", "treasuries", "global"])
CAT_RX = {k: _rx(v[3]) for k, v in CATS.items()}
HIGH_RX = _rx(HIGH)
# عبارت‌هایی که اشتباهاً دسته‌بندی می‌شوند، قبل از تطبیق حذف می‌شوند
_PRECLEAN = re.compile(r"(?i)\b(?:james bond|dollar (?:general|tree|shave club)|gold(?:en)? medal\w*|gold coast)\b")
# نام‌های مبهم داو: فقط وقتی معتبرند که کلمهٔ کسب‌وکار/بازار هم در تیتر باشد
DOW_AMBIG = {"apple", "visa", "travelers", "amazon"}
BIZ_RX = _rx(["shares", "share", "earnings", "stock", "stocks", "revenue", "profit", "quarter", "guidance", "ceo", "analyst",
              "analysts", "upgrade", "downgrade", "iphone", "ipad", "tim cook", "aws", "buyback", "dividend", "market",
              "markets", "nasdaq", "dow", "mastercard", "payments", "insurance", "insurer", "insurers", "wall street"])

# ---------- جهت خبر: برای اینکه «rise» و «fall» اشتباهاً تکراری حساب نشوند ----------
_UP_WORDS = ["rise", "rises", "rose", "rising", "rally", "rallies", "rallied", "jump", "jumps", "jumped",
             "soar", "soars", "soared", "surge", "surges", "surged", "gain", "gains", "gained",
             "climb", "climbs", "climbed", "higher", "advance", "advances", "advanced", "rebound", "rebounds",
             "beat", "beats", "stronger", "tops", "topped", "record high", "all-time high", "hits record",
             "jumped", "spikes", "spiked", "rallies"]
_DOWN_WORDS = ["fall", "falls", "fell", "falling", "drop", "drops", "dropped", "plunge", "plunges", "plunged",
               "slump", "slumps", "slumped", "tumble", "tumbles", "tumbled", "sink", "sinks", "sank",
               "slide", "slides", "slid", "decline", "declines", "declined", "lower", "weaker",
               "miss", "misses", "missed", "dip", "dips", "dipped", "slip", "slips", "slipped", "weakens",
               "selloff", "sell-off", "rout", "crash", "crashes", "crashed", "retreat", "retreats", "drops"]
UP_RX = _rx(_UP_WORDS)
DOWN_RX = _rx(_DOWN_WORDS)
_UP_TOKEN, _DOWN_TOKEN = "_up_", "_down_"


def log(msg):
    line = datetime.now().strftime("[%H:%M:%S] ") + msg
    print(line, flush=True)
    if _LOG_FILE:
        try:
            if os.path.exists(_LOG_FILE) and os.path.getsize(_LOG_FILE) > _LOG_MAX:
                os.replace(_LOG_FILE, _LOG_FILE + ".1")
            with open(_LOG_FILE, "a", encoding="utf-8") as f:
                f.write(datetime.now().strftime("%Y-%m-%d ") + line + "\n")
        except OSError:
            pass


_LOG_FILE = None
_LOG_MAX = 1000000


# ------------------------- تنظیمات -------------------------
def load_cfg():
    cfg = dict(DEFAULTS)
    if not os.path.exists(CFG_FILE):
        print("هنوز تنظیماتی وجود ندارد؛ جادوگر راه‌اندازی شروع می‌شود.")
        if not wizard():
            sys.exit("راه‌اندازی کامل نشد. بعد از رفع مشکل دوباره اجرا کن:  python news_agent.py --setup")
    try:
        with open(CFG_FILE, encoding="utf-8") as f:
            cfg.update(json.load(f))
    except ValueError as e:
        sys.exit(f"فایل تنظیمات خراب است ({CFG_FILE}): {e}\nآن را اصلاح یا حذف کن و دوباره --setup را اجرا کن.")
    try:
        os.chmod(CFG_FILE, 0o600)      # حتی اگر فایل دستی ساخته شده باشد، فقط خودمان بخوانیم
    except Exception:
        pass
    if cfg.get("ai_model") in OLD_AI_MODELS:      # نام مدل نامعتبر نسخهٔ قبل
        cfg["ai_model"] = DEFAULTS["ai_model"]
    cfg["token"] = os.environ.get("TG_BOT_TOKEN", cfg["token"]).strip()
    cfg["channel"] = os.environ.get("TG_CHANNEL", cfg["channel"]).strip()
    cfg["discord_webhook"] = os.environ.get("DISCORD_WEBHOOK_URL", cfg.get("discord_webhook", "")).strip()
    cfg["anthropic_key"] = os.environ.get("ANTHROPIC_API_KEY", cfg.get("anthropic_key", "")).strip()
    if os.environ.get("NEWS_AI_SUMMARY", "").strip().lower() in ("0", "false", "no"):
        cfg["ai_news_summary"] = False
    return cfg


def acquire_lock():
    """جلوگیری از اجرای دو نمونهٔ هم‌زمان (روی ویندوز نادیده گرفته می‌شود)."""
    if os.name == "nt":
        return None
    try:
        import fcntl
    except ImportError:
        return None
    f = open(LOCK_FILE, "w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit("❌ یک نمونهٔ دیگر از ایجنت همین حالا در حال اجراست (قفل: .news_agent.lock).\n"
                 "   اگر مطمئنی نمونه‌ای در حال اجرا نیست، فایل قفل را پاک کن.")
    return f


# ------------------------- شبکه -------------------------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")


def fetch(url, timeout=15, data=None):
    req = urllib.request.Request(url, data=data, headers={
        "User-Agent": UA,
        "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


# ------------------------- RSS / Atom -------------------------
def _txt(el):
    return (el.text or "").strip() if el is not None else ""


def clean(s):
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", s or ""))).strip()


def parse_date(s):
    if not s:
        return None
    try:
        d = parsedate_to_datetime(s)
    except Exception:
        try:
            s2 = re.sub(r"(\.\d{6})\d+", r"\1", s.strip())   # 7 رقم اعشار در sitemap
            d = datetime.fromisoformat(s2.replace("Z", "+00:00"))
        except Exception:
            return None
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d


def parse_feed(xml_bytes):
    root = ET.fromstring(xml_bytes)
    out = []
    for it in root.iter():
        kind = it.tag.split("}")[-1]
        if kind not in ("item", "entry", "url"):
            continue
        d = {"title": "", "link": "", "date": None, "guid": "", "desc": ""}
        if kind == "url":      # نقشهٔ خبری گوگل (sitemap news)
            for ch in it.iter():
                t = ch.tag.split("}")[-1]
                if t == "loc" and not d["link"]:
                    d["link"] = _txt(ch)
                elif t == "title" and not d["title"]:
                    d["title"] = clean(_txt(ch))
                elif t == "publication_date" and d["date"] is None:
                    d["date"] = parse_date(_txt(ch))
            if d["title"]:
                out.append(d)
            continue
        for ch in it:
            t = ch.tag.split("}")[-1]
            if t == "title" and not d["title"]:
                d["title"] = clean(_txt(ch))
            elif t == "link":
                if not d["link"]:
                    d["link"] = _txt(ch) or ch.attrib.get("href", "")
            elif t in ("pubDate", "published", "updated", "date") and d["date"] is None:
                d["date"] = parse_date(_txt(ch))
            elif t in ("guid", "id"):
                d["guid"] = _txt(ch)
            elif t in ("description", "summary") and not d["desc"]:
                d["desc"] = clean(_txt(ch))
        if d["title"]:
            out.append(d)
    return out


# ------------------------- فیلتر -------------------------
def classify(title, include_dow_stocks=True):
    t = _PRECLEAN.sub(" ", title)
    cats = [k for k, rx in CAT_RX.items() if (k != "dowstk" or include_dow_stocks) and rx.search(t)]
    if "dowstk" in cats:
        names = {m.group(0).lower() for m in CAT_RX["dowstk"].finditer(t)}
        if names <= DOW_AMBIG and not BIZ_RX.search(t):
            cats.remove("dowstk")
    # «ریزش ناگهانی» فقط وقتی معتبر است که خبر به بازار/دارایی‌ها هم مربوط باشد
    if "crash" in cats and len(cats) == 1 and not MARKET_RX.search(t):
        cats = []
    return cats, bool(HIGH_RX.search(t))


def norm(title):
    return hashlib.sha1(re.sub(r"[^a-z0-9]+", "", title.lower()).encode()).hexdigest()[:16]


_STOP = set("a an the of to in on as at for and or is are was were be by with from after before over amid its that this".split())


def direction(title):
    """+1 صعودی، -1 نزولی، 0 خنثی/مبهم. برای اینکه rise/fall تکراری حساب نشوند."""
    up, down = bool(UP_RX.search(title or "")), bool(DOWN_RX.search(title or ""))
    if up and not down:
        return 1
    if down and not up:
        return -1
    return 0


# هم‌معنی‌هایی که رسانه‌ها برای یک خبر به‌جای هم می‌نویسند (قبل از مقایسه یکی می‌شوند)
_SYN = {"reiterates": "maintain", "reiterated": "maintain", "maintains": "maintain", "maintained": "maintain",
        "keeps": "maintain", "kept": "maintain", "retains": "maintain", "retained": "maintain", "holds": "maintain",
        "lifts": "raise", "raises": "raise", "raised": "raise", "boosts": "raise", "hikes": "raise", "hiked": "raise",
        "lowers": "lower", "lowered": "lower", "slashes": "lower", "trims": "lower",
        "pt": "target", "targets": "target", "shares": "stock", "stocks": "stock", "says": "say", "said": "say",
        "usd": "dollar", "dollars": "dollar", "greenback": "dollar", "wall": "wallstreet", "ahead": "before"}


def _stem(w):
    w = _SYN.get(w, w)
    if len(w) > 5:
        for suf in ("ing", "ed", "es"):
            if w.endswith(suf):
                return w[: -len(suf)]
    if len(w) > 4 and w.endswith("s") and not w.endswith("ss"):
        return w[:-1]
    return w


# «موضوع» خبر (دارایی یا شرکت). دو تیتر با موضوع‌های متفاوت هرگز تکراری حساب نمی‌شوند
# (مثلاً «Gold rises 1% as dollar weakens» با «Silver rises 1% as dollar weakens»، یا انویدیا با مایکروسافت)
_ENT_ALIAS = {"crude": "oil", "brent": "oil", "wti": "oil", "bitcoin": "btc", "sterling": "gbp", "pound": "gbp",
              "dow jones": "dow", "djia": "dow", "xau": "gold", "xag": "silver", "s&p 500": "sp500"}
_ENT_RX = _rx(["gold", "silver", "oil", "crude", "brent", "wti", "bitcoin", "btc", "ethereum", "nasdaq", "dow jones",
               "dow", "djia", "s&p 500", "euro", "yen", "sterling", "pound", "xau", "xag", "canadian dollar", "loonie"]
              + DOW30)


def _entities(title):
    t = _PRECLEAN.sub(" ", (title or "").lower())
    return {"@" + _ENT_ALIAS.get(m.group(0), m.group(0)) for m in _ENT_RX.finditer(t)}


def title_tokens(title):
    toks = {_stem(w) for w in re.findall(r"[a-z0-9$%.]+", (title or "").lower()) if w not in _STOP and len(w) > 1}
    toks |= _entities(title)
    d = direction(title)
    if d > 0:
        toks.add(_UP_TOKEN)       # نشانهٔ جهت داخل مجموعهٔ واژه‌ها می‌نشیند
    elif d < 0:
        toks.add(_DOWN_TOKEN)
    return toks


def is_similar(a, b, thr):
    if not a or not b:
        return False
    # یک خبر صعودی و یک خبر نزولی هرگز «تکراری» نیستند، حتی اگر بقیهٔ واژه‌ها یکی باشند
    if (_UP_TOKEN in a and _DOWN_TOKEN in b) or (_DOWN_TOKEN in a and _UP_TOKEN in b):
        return False
    ea, eb = {w for w in a if w.startswith("@")}, {w for w in b if w.startswith("@")}
    if ea and eb and ea != eb:
        return False
    inter = len(a & b)
    if inter / len(a | b) >= thr or (inter >= 4 and inter / min(len(a), len(b)) >= 0.8):
        return True
    # یک عدد مشترک (مثل 125 یا 1.5%) + چند واژهٔ مشترک دیگر = به احتمال زیاد همان خبر با بازنویسی متفاوت
    common = (a & b) - {_UP_TOKEN, _DOWN_TOKEN}
    nums = {w for w in common if any(c.isdigit() for c in w) and len(w) >= 2}
    return bool(nums) and len(common) >= 5 and inter / len(a | b) >= 0.4


def item_keys(it):
    k = [norm(it["title"])]
    ident = it["guid"] or it["link"]
    if ident:
        k.append(hashlib.sha1(ident.encode()).hexdigest()[:16])
    return k


def make_summary(desc, title, limit):
    """چکیدهٔ کوتاه از توضیح فید؛ اگر تکرار تیتر یا خیلی کوتاه بود خالی برمی‌گرداند"""
    d = re.sub(r"\s*The post .{0,200}? appeared first on .*$", "", desc or "", flags=re.I).strip()
    if len(d) < 40 or (d.lower().startswith(title.lower()[:60]) and len(d) < len(title) + 40):
        return ""
    if len(d) > limit:
        cut = d[:limit]
        m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
        d = cut[:m + 1] if m > limit * 0.5 else cut.rsplit(" ", 1)[0] + "…"
    return d


def article_text(url, limit=2500):
    """متن ابتدای خبر از صفحهٔ سایت (توضیح متا + پاراگراف‌ها). اگر سایت اجازه نداد، رشتهٔ خالی."""
    try:
        raw = fetch(url, timeout=12).decode("utf-8", "ignore")
    except Exception:
        return ""
    raw = re.sub(r"(?is)<(script|style|noscript|svg|nav|footer|header|aside|form)[^>]*>.*?</\1>", " ", raw)
    meta = ""
    for tag in re.findall(r"(?is)<meta\b[^>]*>", raw):
        if re.search(r"(?i)(property|name)\s*=\s*[\"'](og:description|description)[\"']", tag):
            m = re.search(r"(?is)content\s*=\s*[\"'](.*?)[\"']\s*/?>?$", tag.strip()) or re.search(r"(?is)content\s*=\s*[\"'](.*?)[\"']", tag)
            if m and len(m.group(1)) > len(meta):
                meta = clean(m.group(1))
    paras = [clean(x) for x in re.findall(r"(?is)<p\b[^>]*>(.*?)</p>", raw)]
    paras = [x for x in paras if len(x) > 60 and "cookie" not in x.lower() and "©" not in x]
    body = " ".join(paras)
    text = (meta + " " + body).strip() if meta and not body.startswith(meta[:40]) else (body or meta)
    return text[:limit]


def prefetch_articles(cfg, batch):
    """متن همهٔ خبرهای یک بچ را هم‌زمان می‌خواند (به‌جای ۸ بار ۱۲ ثانیه پشت‌سرهم)."""
    if not (cfg.get("fetch_article") and cfg.get("include_summary")):
        return
    need = [it for it in batch
            if not it.get("_nofetch") and it.get("link") and not it.get("_art")]
    if not need:
        return
    workers = max(1, min(int(cfg.get("article_workers", 6)), len(need)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for it, txt in zip(need, pool.map(lambda x: article_text(x["link"]), need)):
            it["_art"] = txt or ""


def ai_summary(cfg, title, text, source):
    """چکیدهٔ کوتاه فارسی با Claude (اختیاری). در صورت کافی نبودن متن، رشتهٔ خالی."""
    system = ("تو دستیار خبری یک کانال بازارهای مالی هستی. از روی تیتر و متن خبر انگلیسی، یک چکیدهٔ فارسی روان و کوتاه بنویس: "
              "حداکثر ۲ جمله و حداکثر ۲۵۰ حرف؛ چه اتفاقی افتاد، اعداد و نام‌های کلیدی، و اگر از خود متن روشن است اثر احتمالی "
              "روی بازار (طلا، دلار، سهام و...). چیزی از خودت اضافه نکن، توصیهٔ خرید و فروش نده، و جمله‌ها را از متن کپی نکن. "
              "اگر متن برای چکیدهٔ مفید کافی نیست فقط بنویس NONE. فقط خود چکیده را بنویس.")
    body = {"model": cfg["ai_model"], "max_tokens": 300, "system": system,
            "messages": [{"role": "user", "content": f"Source: {source}\nTitle: {title}\nText: {text[:3000]}"}]}
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode("utf-8"),
                                 headers={"x-api-key": cfg["anthropic_key"], "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            data = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error", {}).get("message", "")
        except Exception:
            msg = ""
        raise RuntimeError(f"Anthropic HTTP {e.code}: {msg[:120]}")
    out = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()
    return "" if (not out or out.upper().startswith("NONE")) else out


def get_summary(cfg, it):
    """چکیدهٔ نهایی خبر (فارسی اگر ترجمه/AI فعال باشد)؛ اگر چیزی مفید نبود رشتهٔ خالی."""
    if not cfg["include_summary"]:
        return ""
    if it.get("_nofetch") and not cfg.get("summary_restricted_sources"):
        return ""      # Reuters / FT / Bloomberg / Google News: فقط تیتر + لینک
    summ = make_summary(it["desc"], it["title"], cfg["summary_chars"])
    base = it["desc"]
    use_ai = bool(cfg.get("anthropic_key")) and cfg.get("ai_news_summary", True)
    art = it.get("_art") or ""       # معمولاً از قبل و به‌صورت هم‌زمان گرفته شده
    if (not summ or use_ai) and cfg.get("fetch_article") and it["link"] and not it.get("_nofetch"):
        if not art:
            art = article_text(it["link"])
        if art:
            if len(art) > len(base):
                base = art
            if not summ:
                summ = make_summary(art, it["title"], cfg["summary_chars"])
    if use_ai and (summ or len(base) > 60):
        try:
            ai = ai_summary(cfg, it["title"], base, it.get("_src", ""))
            if ai:
                return ai            # از قبل فارسی است
            if not summ:
                return ""
        except Exception as e:  # noqa
            log(f"چکیدهٔ هوشمند ناموفق (چکیدهٔ ساده استفاده می‌شود): {str(e)[:100]}")
    if summ and cfg["translate"]:
        try:
            summ = translate_fa(summ)
        except Exception as e:  # noqa
            log(f"ترجمهٔ چکیده ناموفق (چکیده ارسال نمی‌شود): {str(e)[:60]}")
            return ""    # چکیدهٔ انگلیسی هرگز ارسال نمی‌شود
    return summ


# ------------------------- ترجمه -------------------------
_tr_cache = {}


def _clip_text(text, n=1000):
    """سقف طول متن برای URL سرویس‌های ترجمه (محدودیت ~۲KB گوگل)."""
    text = (text or "").strip()
    if len(text) <= n:
        return text
    cut = text[:n]
    m = max(cut.rfind(". "), cut.rfind("! "), cut.rfind("? "))
    return cut[:m + 1] if m > n * 0.4 else cut.rsplit(" ", 1)[0]


def _tr_google(text):
    url = "https://translate.googleapis.com/translate_a/single?client=gtx&sl=en&tl=fa&dt=t&q=" + urllib.parse.quote(_clip_text(text))
    data = json.loads(fetch(url, timeout=10).decode("utf-8"))
    return "".join(seg[0] for seg in data[0] if seg and seg[0]).strip()


def _tr_google2(text):
    url = "https://clients5.google.com/translate_a/t?client=dict-chrome-ex&sl=en&tl=fa&q=" + urllib.parse.quote(_clip_text(text))
    data = json.loads(fetch(url, timeout=10).decode("utf-8"))
    if not isinstance(data, list) or not data:
        return ""
    first = data[0]
    if isinstance(first, str):                    # پاسخ تک‌رشته‌ای
        return first.strip()
    # پاسخ چندقطعه‌ای: همهٔ قطعه‌ها باید به هم چسبانده شوند (باگ نسخهٔ قبل: فقط قطعهٔ اول)
    return "".join(seg[0] for seg in data if isinstance(seg, list) and seg and isinstance(seg[0], str)).strip()


def _tr_lingva(text):
    url = "https://lingva.ml/api/v1/en/fa/" + urllib.parse.quote(_clip_text(text), safe="")
    return json.loads(fetch(url, timeout=12).decode("utf-8")).get("translation", "").strip()


def _tr_mymemory(text):
    url = "https://api.mymemory.translated.net/get?langpair=en|fa&q=" + urllib.parse.quote(_clip_text(text)[:450])
    data = json.loads(fetch(url, timeout=10).decode("utf-8"))
    return html.unescape(data.get("responseData", {}).get("translatedText", "")).strip()


def _has_persian(t):
    t = t or ""
    if not t:
        return False
    persian = len(re.findall(r"[\u0600-\u06FF]", t))
    return persian >= 3 and persian >= len(t) / 6


class TranslateDown(RuntimeError):
    """سرویس‌های ترجمه به‌تازگی همه شکست خورده‌اند؛ چند ثانیه امتحان نمی‌شوند."""


_tr_down_until = 0.0


def translate_fa(text):
    """ترجمهٔ فارسی؛ چند سرویس را به‌ترتیب امتحان می‌کند. اگر همه شکست خوردند Exception می‌دهد (هرگز انگلیسی برنمی‌گرداند)."""
    global _tr_down_until
    if text in _tr_cache:
        return _tr_cache[text]
    if time.time() < _tr_down_until:
        raise TranslateDown("سرویس‌های ترجمه موقتاً کنار گذاشته شده‌اند")
    last = None
    for _ in range(2):
        for fn in (_tr_google, _tr_google2, _tr_lingva, _tr_mymemory):
            try:
                res = fn(text)
                if res and _has_persian(res):
                    if len(_tr_cache) > 2000:
                        _tr_cache.clear()
                    _tr_cache[text] = res
                    return res
            except Exception as e:  # noqa
                last = e
        time.sleep(1.5)
    _tr_down_until = time.time() + 60      # یک دقیقه بقیهٔ خبرها را معطل نکن
    raise RuntimeError(f"همهٔ سرویس‌های ترجمه شکست خوردند: {str(last)[:80]}")


# ------------------------- پیام -------------------------
def esc(s):
    return html.escape(s, quote=False)


# اگر zoneinfo/tzdata روی سیستم نبود (مثلاً بعضی ویندوزها یا کانتینرهای مینیمال)، این افست‌های ثابت جایگزین می‌شوند.
# ایران از ۱۴۰۱ ساعت تابستانی ندارد؛ همیشه UTC+03:30.
_FIXED_TZ = {"asia/tehran": (3, 30), "iran": (3, 30), "utc": (0, 0), "europe/berlin": (1, 0), "europe/london": (0, 0)}


def local_time(dt, tzname):
    """ساعت خبر به وقت نمایش (پیش‌فرض تهران)؛ اگر zoneinfo نبود، افست ثابت؛ در نهایت ساعت سیستم."""
    if not dt:
        return ""
    if tzname:
        try:
            from zoneinfo import ZoneInfo
            return dt.astimezone(ZoneInfo(tzname)).strftime("%H:%M")
        except Exception:
            pass
        fx = _FIXED_TZ.get(tzname.strip().lower())
        if fx:
            from datetime import timedelta
            return dt.astimezone(timezone(timedelta(hours=fx[0], minutes=fx[1]))).strftime("%H:%M")
    return dt.astimezone().strftime("%H:%M")


RLM = "\u200f"      # علامت راست‌به‌چپ (نامرئی)
# هر قطعهٔ لاتین (نام شرکت، عدد، $، %، ...) که وسط یا ابتدای متن فارسی آمده است
_LATIN_RUN = re.compile(r"[A-Za-z0-9$%][A-Za-z0-9$%.,'’&/+\-:()#@ ]*[A-Za-z0-9$%)]|[A-Za-z0-9$%]")


def fix_rtl(text):
    """متن فارسیِ دارای کلمات انگلیسی را در تلگرام درست نمایش می‌دهد.
    مشکل: اگر خط با کلمهٔ انگلیسی (مثل TD Cowen) شروع شود، تلگرام کل خط را چپ‌به‌راست می‌چیند و جملهٔ فارسی به‌هم می‌ریزد.
    راه‌حل: یک RLM اول خط، و یک RLM بعد از هر قطعهٔ لاتین تا علامت‌ها و اعداد بعدی جابه‌جا نشوند.
    (فقط روی متن‌های فارسی اعمال می‌شود.)"""
    if not text or not _has_persian(text):
        return text
    text = text.replace(RLM, "")
    return RLM + _LATIN_RUN.sub(lambda m: m.group(0) + RLM, text)


def build_message(it, cats, high, fa, show_orig, source, summary="", tzname=None):
    labels = " ".join(f"{CATS[c][0]} {CATS[c][1]}" for c in cats)
    tags = " ".join(CATS[c][2] for c in cats)
    head = ("🔴 مهم | " if high else "") + labels
    title_txt = fix_rtl(fa) if fa else it["title"]      # فقط ترجمهٔ فارسی؛ تیتر انگلیسی اصلی دست نمی‌خورد
    lines = [RLM + head, "", f"{RLM if fa else ''}<b>{esc(title_txt)}</b>"]
    if show_orig and fa and fa != it["title"]:
        lines.append(f"<i>{esc(it['title'])}</i>")
    if summary:
        lines.append("")
        lines.append(esc(fix_rtl(summary)))
    when = local_time(it["date"], tzname)
    lines.append("")
    lines.append(f"📰 {esc(source)}" + (f" · 🕒 {when}" if when else ""))
    if it["link"]:
        lines.append(f'🔗 <a href="{html.escape(it["link"], quote=True)}">لینک خبر</a>')
    lines.append(RLM + tags)
    return "\n".join(lines)


def tg_api(token, method, params=None):
    """فراخوانی ساده API تلگرام؛ همیشه یک dict برمی‌گرداند (ok / description)"""
    url = f"https://api.telegram.org/bot{token}/{method}"
    body = urllib.parse.urlencode(params or {}).encode()
    try:
        return json.loads(fetch(url, timeout=20, data=body).decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read().decode("utf-8"))
        except Exception:
            return {"ok": False, "description": f"HTTP {e.code}"}
    except Exception as e:  # noqa  (قطعی اینترنت / فیلتر)
        return {"ok": False, "description": f"اتصال به تلگرام ممکن نشد: {e}"}


def explain_tg_error(desc):
    d = (desc or "").lower()
    if "can't send messages to the bot" in d or "bot can't send" in d:
        return ("مقصد را آیدی یک «ربات» گذاشته‌ای. مقصد باید یک کانال (@نام_کانال) یا آیدی عددی چت خودت باشد، نه ربات.")
    if "chat not found" in d:
        return "مقصد پیدا نشد: یوزرنیم کانال را اشتباه نوشته‌ای، یا ربات هنوز در آن کانال ادمین نیست."
    if "not enough rights" in d or "administrator" in d or "member of the channel" in d:
        return "ربات در کانال ادمین نیست یا اجازهٔ Post Messages ندارد."
    if "message is too long" in d:
        return "متن پیام از سقف ۴۰۹۶ کاراکتر تلگرام بیشتر شد؛ summary_chars را کمتر کن."
    if "unauthorized" in d or "not found" == d.strip():
        return "توکن اشتباه است. دوباره از BotFather کپی کن (کامل، با «:» وسطش)."
    if "اتصال" in d or "timed out" in d or "urlopen" in d:
        return "تلگرام از اینترنت تو باز نمی‌شود. فیلترشکن را روشن کن (ترجیحاً روی کل سیستم)."
    return ""


def looks_like_bot(channel):
    c = (channel or "").strip().lower()
    return c.startswith("@") and c.endswith("bot")


def wizard():
    """جادوگر راه‌اندازی تعاملی. در صورت موفقیت تنظیمات را ذخیره می‌کند و True برمی‌گرداند."""
    print("\n=== راه‌اندازی ربات خبری ===")
    print("توکن را از @BotFather بگیر (شبیه 123456789:ABCdef...) و اینجا Paste کن.")
    token = getpass.getpass("توکن ربات (موقع Paste چیزی نمایش داده نمی‌شود): ").strip()
    r = tg_api(token, "getMe")
    if not r.get("ok"):
        print("❌ توکن معتبر نیست: " + (explain_tg_error(r.get("description")) or str(r.get("description"))))
        return False
    bot = r["result"]["username"]
    print(f"✅ ربات پیدا شد: @{bot}")
    print("\nخبرها کجا ارسال شوند؟\n  1) در یک کانال تلگرام (ربات باید در کانال ادمین باشد)\n"
          "  2) در چت خصوصی خودم با ربات")
    choice = input("عدد 1 یا 2: ").strip()
    if choice == "2":
        input(f"\nالان در تلگرام به @{bot} یک پیام /start بفرست، بعد اینجا Enter بزن...")
        r = tg_api(token, "getUpdates")
        chat_id = None
        for u in reversed(r.get("result", []) if r.get("ok") else []):
            m = u.get("message") or u.get("edited_message") or {}
            if m.get("chat", {}).get("type") == "private":
                chat_id = str(m["chat"]["id"])
                break
        if not chat_id:
            print("❌ پیامی پیدا نشد. مطمئن شو /start را به همین ربات فرستاده‌ای و دوباره --setup را اجرا کن.")
            return False
        channel = chat_id
        print(f"✅ آیدی چت تو پیدا شد: {chat_id}")
    else:
        print("\nیوزرنیم کانال را بنویس (مثل @my_news_channel). این یوزرنیم «کانال» است، نه ربات.")
        channel = input("کانال: ").strip()
        if channel and not channel.startswith(("@", "-")) and not channel.isdigit():
            channel = "@" + channel
        if looks_like_bot(channel):
            print("❌ این یوزرنیم یک ربات به نظر می‌رسد (آخرش bot دارد). یوزرنیم کانال را بده.")
            return False
    r = tg_api(token, "sendMessage", {"chat_id": channel, "text": "✅ ربات خبری بازار با موفقیت وصل شد."})
    if not r.get("ok"):
        desc = r.get("description", "")
        print(f"❌ ارسال پیام تست ناموفق بود: {desc}")
        hint = explain_tg_error(desc)
        if hint:
            print("   راهنما: " + hint)
        return False
    cfg = {}
    if os.path.exists(CFG_FILE):
        with open(CFG_FILE, encoding="utf-8") as f:
            cfg = json.load(f)
    full = dict(DEFAULTS)
    full.update(cfg)
    full["token"], full["channel"] = token, channel
    with open(CFG_FILE, "w", encoding="utf-8") as f:
        json.dump(full, f, ensure_ascii=False, indent=2)
    try:
        os.chmod(CFG_FILE, 0o600)      # فقط خودت بتوانی بخوانی
    except Exception:
        pass
    print("✅ پیام تست در تلگرام ارسال شد و تنظیمات ذخیره شد.\n")
    return True


DC_MAX = 3900            # حد توضیح Embed دیسکورد ۴۰۹۶ کاراکتر است


def html_to_discord(t, embed=False):
    """HTML تلگرام (b/i/a) را به مارک‌داون دیسکورد تبدیل می‌کند."""
    t = re.sub(r"</?b>", "**", t)
    t = re.sub(r"</?i>", "*", t)
    t = re.sub(r"<(?!/?a[ >])[^>]+>", "", t)          # هر تگ دیگری حذف شود (a می‌ماند)
    t = re.sub(r'<a href="([^"]+)">(.*?)</a>', lambda m: (f"[{m.group(2)}]({html.unescape(m.group(1))})" if embed
                                                              else f"[{m.group(2)}](<{html.unescape(m.group(1))}>)"), t, flags=re.S)
    return html.unescape(t)


def send_discord(webhook, text):
    text = html_to_discord(text, embed=True)
    color = 0xE74C3C if "🔴" in text[:60] else (0x9B59B6 if "📅" in text[:20] else 0x3498DB)
    parts, cur = [], ""
    for ln in text.split("\n"):
        while len(ln) > DC_MAX:
            if cur:
                parts.append(cur)
                cur = ""
            parts.append(ln[:DC_MAX])
            ln = ln[DC_MAX:]
        if cur and len(cur) + 1 + len(ln) > DC_MAX:
            parts.append(cur)
            cur = ln
        else:
            cur = f"{cur}\n{ln}" if cur else ln
    if cur.strip():
        parts.append(cur)
    for part in parts:
        body = json.dumps({"embeds": [{"description": part, "color": color}],
                           "allowed_mentions": {"parse": []}}).encode()
        for _ in range(3):
            req = urllib.request.Request(webhook, data=body, headers={
                "Content-Type": "application/json", "User-Agent": "DiscordBot (news-agent, 1.0)"})
            try:
                urllib.request.urlopen(req, timeout=20).read()
                break
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    try:
                        wait = float(json.loads(e.read().decode()).get("retry_after", 2))
                    except Exception:
                        wait = 2
                    time.sleep(wait + 0.5)
                    continue
                raise RuntimeError(f"دیسکورد خطا داد ({e.code}): {e.reason}")
            except (urllib.error.URLError, OSError) as e:
                if _ == 2:
                    raise RuntimeError(f"اتصال به دیسکورد ممکن نشد: {e}")
                time.sleep(2)
        else:
            raise RuntimeError("دیسکورد: Too Many Requests")
        time.sleep(0.4)
    return True


def make_post(cfg):
    if cfg.get("discord_webhook"):
        return lambda m: send_discord(cfg["discord_webhook"], m)
    return lambda m: send_telegram(cfg["token"], cfg["channel"], m)


def send_telegram(token, channel, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = urllib.parse.urlencode({"chat_id": channel, "text": text, "parse_mode": "HTML",
                                   "disable_web_page_preview": "true"}).encode()
    last = "نامشخص"
    for _ in range(3):
        try:
            raw = fetch(url, timeout=20, data=body).decode("utf-8", "ignore")
            try:
                j = json.loads(raw)
            except ValueError:
                return True          # پاسخ غیر JSON ولی HTTP موفق
            if j.get("ok"):
                return True
            last = str(j.get("description", "ok=false"))
            if "too many requests" in last.lower():      # 429 با بدنهٔ 200
                time.sleep(int(j.get("parameters", {}).get("retry_after", 5)) + 1)
                continue
            raise RuntimeError(f"تلگرام خطا داد: {last}")
        except urllib.error.HTTPError as e:
            try:
                j = json.loads(e.read().decode())
            except Exception:
                j = {}
            if e.code == 429:
                last = "429 Too Many Requests"
                time.sleep(int(j.get("parameters", {}).get("retry_after", 5)) + 1)
                continue
            raise RuntimeError(f"تلگرام خطا داد ({e.code}): {j.get('description', e.reason)}")
        except (urllib.error.URLError, OSError) as e:      # قطعی لحظه‌ای اینترنت/فیلتر
            last = f"اتصال به تلگرام ممکن نشد: {e}"
            time.sleep(2)
    raise RuntimeError(f"ارسال به تلگرام ناموفق بود: {last}")


# ------------------------- استیت (دیده‌شده‌ها + تیترهای اخیر) -------------------------
# چیزهای اضافه‌ای که در همان فایل وضعیت ذخیره می‌شوند (در GitHub Actions فقط همین یک فایل کش می‌شود)
_STATE_EXTRA = {"analysis_sent": []}      # شناسهٔ تحلیل‌های روزانهٔ ارسال‌شده، مثل "2026-11-20 10:00"


def load_state(path=None):
    """(seen, recent) — سازگار با فرمت قدیمی (فقط لیست کلیدها). recent = لیست مجموعهٔ واژه‌ها."""
    path = path or SEEN_FILE
    _STATE_EXTRA["analysis_sent"] = []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return None, []
    except Exception as e:  # JSON خراب: بی‌صدا صفر نکن، نسخهٔ خراب را کنار بگذار
        try:
            os.replace(path, path + ".corrupt")
        except OSError:
            pass
        log(f"⚠ فایل وضعیت خوانده نشد ({str(e)[:80]})؛ نسخهٔ خراب به {os.path.basename(path)}.corrupt منتقل شد.")
        return None, []
    if isinstance(data, list):            # فرمت قدیمی
        return data, []
    seen = data.get("seen") or []
    _STATE_EXTRA["analysis_sent"] = [x for x in (data.get("analysis_sent") or []) if isinstance(x, str)][-30:]
    recent = [set(t) for t in (data.get("recent") or []) if isinstance(t, list)]
    return seen, recent[-RECENT_KEEP:]


def save_state(seen, recent, path=None):
    path = path or SEEN_FILE
    payload = {"version": STATE_VERSION,
               "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "analysis_sent": _STATE_EXTRA["analysis_sent"][-30:],
               "seen": seen[-SEEN_KEEP:],
               "recent": [sorted(t) for t in recent[-RECENT_KEEP:]]}
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def prune(seen):
    """جلوگیری از رشد نامحدود حافظه در اجرای طولانی."""
    if len(seen) > SEEN_KEEP + 2000:
        del seen[:-SEEN_KEEP]


# ------------------------- یک دور بررسی -------------------------
_warned = set()
_tr_fails = {}
_send_fails = {}


def _mark_seen(seen, it):
    seen.extend(it["_keys"])


def run_cycle(cfg, seen, post_fn, get_feed=None, first=False, backfill=0, recent=None, save_fn=None):
    """seen: لیست کلیدها (تغییر می‌کند). خروجی: تعداد پیام‌های ارسال‌شده"""
    get_feed = get_feed or (lambda url: parse_feed(fetch(url)))
    seen_set = set(seen)
    recent = recent if recent is not None else []      # مجموعهٔ واژه‌های تیترهای منتشرشده (برای تکراری فازی)
    thr = cfg.get("similar_threshold", 0.6)
    now = datetime.now(timezone.utc)
    fresh, ok_feeds = [], 0

    all_feeds = list(FEEDS) + [tuple(x) for x in cfg.get("extra_feeds", []) if len(x) == 2]
    if not cfg.get("use_google_news", True):          # فیدهای گوگل قابل خاموش کردن‌اند
        all_feeds = [f for f in all_feeds if "news.google.com" not in f[1]]

    def _load(nu):
        try:
            return nu, get_feed(nu[1]), None
        except Exception as e:  # noqa
            return nu, None, e

    with ThreadPoolExecutor(max_workers=max(1, int(cfg.get("fetch_workers", 8)))) as pool:
        loaded = list(pool.map(_load, all_feeds))      # فیدها هم‌زمان خوانده می‌شوند (ترتیب حفظ می‌شود)
    for (name, url), items, err in loaded:
        if err is not None:
            if url not in _warned:       # هر فید خراب فقط یک بار گزارش می‌شود
                log(f"⚠ فید «{name}» در دسترس نیست ({url}): {str(err)[:80]}")
                _warned.add(url)
            continue
        ok_feeds += 1
        _warned.discard(url)
        is_gn = "news.google.com" in url
        for it in items:
            if is_gn:       # Google News: پسوند « - Reuters» از تیتر حذف و چکیدهٔ بی‌ارزش فید پاک می‌شود
                m = re.search(r"\s+[-–—]\s+([^-–—]{2,40})$", it["title"])
                if m:
                    it["_pub"] = m.group(1).strip()
                    it["title"] = it["title"][:m.start()].strip()
                it["desc"] = ""
            keys = item_keys(it)
            if any(k in seen_set for k in keys):
                continue
            it["_keys"], it["_src"] = keys, name
            it["_nofetch"] = is_gn or name.startswith(NOFETCH_PREFIX)
            if it.get("_pub"):
                it["_src"] = it["_pub"]
            fresh.append(it)
    # حذف تکراری‌های همین دور (یک خبر در چند فید)
    uniq, got, toks = [], set(), []
    for it in fresh:
        if any(k in got for k in it["_keys"]):
            continue
        t = title_tokens(it["title"])
        if any(is_similar(t, o, thr) for o in toks) or any(is_similar(t, o, thr) for o in recent):
            seen.extend(it["_keys"])      # خبر مشابه قبلاً منتشر شده
            continue
        got.update(it["_keys"])
        it["_tok"] = t
        toks.append(t)
        uniq.append(it)
    uniq.sort(key=lambda x: x["date"] or now)

    base_age = cfg.get("backfill_max_age_minutes", 360) if (first and backfill) else cfg["max_age_minutes"]
    high_age = max(base_age, cfg.get("high_max_age_minutes", base_age))
    relevant = []
    for it in uniq:
        cats, high = classify(it["title"], cfg["include_dow_stocks"])
        if it["_src"] in FORCE_MACRO_HIGH:
            if "macro" not in cats:
                cats.append("macro")
            high = True
        age_min = (now - it["date"]).total_seconds() / 60 if it["date"] else 10**6   # بدون تاریخ = منتشر نشود
        window = high_age if high else base_age     # خبر «مهم» در شلوغی بازار زودتر از دست نمی‌رود
        if not cats or age_min > window or age_min < -10 or (cfg.get("only_high") and not high):
            seen.extend(it["_keys"])
            continue
        it["_cats"], it["_high"] = cats, high
        relevant.append(it)

    if first:   # اولین اجرا: چیزی منتشر نمی‌کنیم مگر backfill خواسته باشد
        for it in relevant[:-backfill] if backfill else relevant:
            seen.extend(it["_keys"])
        relevant = relevant[-backfill:] if backfill else []

    # صف ارسال: مهم‌ترین و تازه‌ترین اول (قبلاً ترتیب فقط زمانی بود و خبر مهم می‌توانست جا بماند)
    queue = sorted(relevant, key=lambda x: (not x["_high"], -(x["date"] or now).timestamp()))
    batch = queue[: cfg["max_per_cycle"]]
    prefetch_articles(cfg, batch)      # متن خبرها هم‌زمان، نه پشت‌سرهم

    sent = 0
    for it in batch:
        fa = None
        if cfg["translate"]:
            try:
                fa = translate_fa(it["title"])
            except TranslateDown:
                log("سرویس ترجمه موقتاً در دسترس نیست؛ بقیهٔ خبرها دور بعد بررسی می‌شوند.")
                break
            except Exception as e:  # noqa
                k0 = it["_keys"][0]
                _tr_fails[k0] = _tr_fails.get(k0, 0) + 1
                if _tr_fails[k0] >= SEND_MAX_FAILS:      # یک تیتر مشکل‌دار نباید صف را برای همیشه ببندد
                    seen.extend(it["_keys"])
                    _tr_fails.pop(k0, None)
                    log("این خبر بعد از ۵ بار ترجمهٔ ناموفق کنار گذاشته شد.")
                # ترجمه نشد: انگلیسی ارسال نمی‌کنیم؛ علامت نمی‌خورد تا دور بعد دوباره تلاش شود (تا وقتی تازه است)
                log(f"ترجمه ناموفق، این خبر فعلاً ارسال نمی‌شود: {str(e)[:60]}")
                continue
        summ = get_summary(cfg, it)
        msg = build_message(it, it["_cats"], it["_high"], fa, cfg["show_original"], it["_src"], summ,
                            cfg.get("display_tz"))
        try:
            post_fn(msg)
        except Exception as e:  # noqa
            k0 = it["_keys"][0]
            _send_fails[k0] = _send_fails.get(k0, 0) + 1
            log(f"ارسال ناموفق ({str(e)[:100]}) {explain_tg_error(str(e))}")
            if _send_fails[k0] >= SEND_MAX_FAILS:        # سقف تلاش: خبر سمی صف را تا ابد معطل نمی‌کند
                seen.extend(it["_keys"])
                _send_fails.pop(k0, None)
                log("این خبر بعد از ۵ بار ارسال ناموفق کنار گذاشته شد.")
            continue
        _send_fails.pop(it["_keys"][0], None)
        seen.extend(it["_keys"])
        recent.append(it["_tok"])
        del recent[:-RECENT_KEEP]
        if save_fn:
            save_fn()          # بلافاصله ذخیره می‌شود تا با کرش/ری‌استارت تکراری نرود
        sent += 1
        time.sleep(3)   # فاصله بین پیام‌ها
    prune(seen)
    if ok_feeds == 0:
        log("هیچ فیدی در دسترس نبود (اینترنت/فیلترشکن؟)")
    return sent


# ------------------------- تحلیل فاندامنتال روزانه -------------------------
# (کلید، ایموجی، نام فارسی، واژه‌های جست‌وجو در تیترها)
ANALYSIS_ASSETS = [
    ("gold",   "🟡", "طلا",          CATS["gold"][3]),
    ("silver", "⚪", "نقره",         CATS["silver"][3]),
    ("nasdaq", "💻", "نزدک",         CATS["nasdaq"][3]),
    ("dow",    "📊", "داو جونز",     ["dow jones", "dow", "djia", "wall street", "s&p 500", "stock market", "stocks"]),
    ("oil",    "🛢️", "نفت",          CATS["oil"][3]),
    ("btc",    "🟠", "بیت‌کوین",     CATS["btc"][3]),
    ("usd",    "💵", "دلار آمریکا",  CATS["usd"][3]),
    ("gbp",    "💷", "پوند",         CATS["gbp"][3]),
    ("cad",    "🍁", "دلار کانادا",  ["canadian dollar", "loonie", "usd/cad", "usdcad", "cad", "bank of canada", "canadian economy"]),
]
ANALYSIS_RX = {k: _rx(w) for k, _e, _n, w in ANALYSIS_ASSETS}
# عوامل عمومیِ اثرگذار روی همهٔ دارایی‌ها (فد، تورم، بازدهی، ترس بازار، ژئوپلیتیک)
DRIVER_RX = _rx(CATS["macro"][3] + CATS["bond"][3] + CATS["vix"][3] + CATS["war"][3])
TG_MAX = 3800            # سقف امن طول هر پیام تلگرام (حد واقعی ۴۰۹۶)
_analysis_fails = {}


def tz_now(tzname, now=None):
    """زمان فعلی در منطقهٔ زمانی داده‌شده (اگر zoneinfo نبود، افست ثابت)."""
    now = now or datetime.now(timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return now.astimezone(ZoneInfo(tzname))
    except Exception:
        fx = _FIXED_TZ.get((tzname or "").strip().lower(), (3, 30))
        return now.astimezone(timezone(timedelta(hours=fx[0], minutes=fx[1])))


def due_analysis_slot(cfg, sent, now=None):
    """(شناسهٔ نوبت یا None, زمان محلی). نوبت وقتی «سررسید» است که ساعتش رسیده، هنوز ارسال نشده و از grace نگذشته باشد."""
    loc = tz_now(cfg.get("analysis_tz") or "Asia/Tehran", now)
    grace = int(cfg.get("analysis_grace_minutes", 90))
    due = None
    for t in sorted(str(x) for x in cfg.get("analysis_times", [])):
        try:
            hh, mm = (int(v) for v in t.split(":"))
            slot = loc.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except ValueError:
            continue
        sid = slot.strftime("%Y-%m-%d %H:%M")
        if sid not in sent and 0 <= (loc - slot).total_seconds() < grace * 60:
            due = sid
    return due, loc


def claude_text(cfg, system, user, max_tokens=3500, model=None, web_search=False, timeout=180):
    """یک پرسش به Claude؛ متن پاسخ را برمی‌گرداند."""
    body = {"model": model or cfg["ai_model"], "max_tokens": max_tokens, "system": system,
            "messages": [{"role": "user", "content": user}]}
    if web_search:
        body["tools"] = [{"type": "web_search_20250305", "name": "web_search",
                          "max_uses": int(cfg.get("analysis_web_searches", 6))}]
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=json.dumps(body).encode("utf-8"),
                                 headers={"x-api-key": cfg["anthropic_key"], "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            msg = json.loads(e.read().decode("utf-8")).get("error", {}).get("message", "")
        except Exception:
            msg = ""
        raise RuntimeError(f"Anthropic HTTP {e.code}: {msg[:160]}")
    return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()


def collect_headlines(cfg, get_feed=None, hours=24, now=None):
    """تیترهای `hours` ساعت اخیر از همهٔ فیدها (بدون توجه به seen). خروجی: [{title, src, date}] تازه‌ترین اول."""
    get_feed = get_feed or (lambda url: parse_feed(fetch(url)))
    now = now or datetime.now(timezone.utc)
    feeds = list(FEEDS) + [tuple(x) for x in cfg.get("extra_feeds", []) if len(x) == 2]
    if not cfg.get("use_google_news", True):
        feeds = [f for f in feeds if "news.google.com" not in f[1]]

    def _load(nu):
        try:
            return nu, get_feed(nu[1])
        except Exception:      # noqa
            return nu, []

    with ThreadPoolExecutor(max_workers=max(1, int(cfg.get("fetch_workers", 8)))) as pool:
        loaded = list(pool.map(_load, feeds))
    out, toks = [], []
    for (name, url), items in loaded:
        is_gn = "news.google.com" in url
        for it in items:
            title, src = it["title"], name
            if is_gn:
                m = re.search(r"\s+[-–—]\s+([^-–—]{2,40})$", title)
                if m:
                    src, title = m.group(1).strip(), title[:m.start()].strip()
            d = it.get("date")
            if not d or not (-10 <= (now - d).total_seconds() / 60 <= hours * 60):
                continue
            t = title_tokens(title)
            if any(is_similar(t, o, 0.6) for o in toks):
                continue
            toks.append(t)
            out.append({"title": title, "src": src, "date": d})
    out.sort(key=lambda x: x["date"], reverse=True)
    return out


def _fmt_headline(h, now):
    hrs = max(0, (now - h["date"]).total_seconds() / 3600)
    return f"- [{hrs:.0f}h ago | {h['src']}] {h['title']}"


def build_analysis_prompt(cfg, heads, loc, now=None):
    now = now or datetime.now(timezone.utc)
    per = int(cfg.get("analysis_headlines_per_asset", 8))
    parts = [f"Current time: {loc.strftime('%Y-%m-%d %H:%M')} ({cfg.get('analysis_tz') or 'Asia/Tehran'})"]
    for key, _e, _n, _w in ANALYSIS_ASSETS:
        rx = ANALYSIS_RX[key]
        rel = [h for h in heads if rx.search(_PRECLEAN.sub(" ", h["title"]))][:per]
        parts.append(f"\n=== {key} ===\n" + ("\n".join(_fmt_headline(h, now) for h in rel) if rel else "(no recent headlines)"))
    drv = [h for h in heads if DRIVER_RX.search(_PRECLEAN.sub(" ", h["title"]))][:per + 4]
    parts.append("\n=== drivers (Fed, inflation, yields, geopolitics, volatility) ===\n"
                 + ("\n".join(_fmt_headline(h, now) for h in drv) if drv else "(no recent headlines)"))
    return "\n".join(parts)


def analysis_system(cfg):
    keys = " ".join(k for k, _e, _n, _w in ANALYSIS_ASSETS)
    web = (" اگر ابزار جستجوی وب در اختیارت است، برای هر دارایی قیمت و خبر همین امروز را هم بررسی کن و فقط اعدادی بنویس که در منبع دیده‌ای."
           if cfg.get("analysis_web_search") else
           " فقط از روی تیترهای داده‌شده بنویس و هیچ قیمت یا سطح عددی از خودت نساز؛ فقط اعدادی که در تیترها آمده‌اند مجازند.")
    return ("تو تحلیلگر فاندامنتال یک کانال فارسی‌زبان بازارهای مالی هستی." + web +
            " برای هر دارایی ۲ تا ۴ جمله بنویس: مهم‌ترین عوامل بنیادی (سیاست پولی فد و بانک‌های مرکزی، تورم و داده‌های اقتصادی، "
            "بازدهی اوراق، قدرت دلار، عرضه و تقاضا، ژئوپلیتیک و ...) و در پایان سوگیری کوتاه‌مدت را با یکی از واژه‌های "
            "«صعودی»، «نزولی» یا «خنثی» و دلیل کوتاه بگو. اگر برای دارایی خبر تازهٔ مهمی نبود، همین را صریح بنویس و فقط "
            "عوامل بنیادی پایدار آن را در یک یا دو جمله بگو. اطلاعات اضافی را حدس نزن. توصیهٔ خرید و فروش نده. "
            "فارسی روان بنویس؛ نام‌هایی مثل Fed و OPEC را می‌توانی انگلیسی بنویسی. "
            f"قالب خروجی دقیقاً این است: برای هر دارایی یک خط جدا به شکل «### کلید» و زیرش متن تحلیل. کلیدها به همین ترتیب: {keys} "
            "و در آخر «### summary» با جمع‌بندی ۲ تا ۳ جمله‌ای از وضعیت کلی بازار. از مارک‌داون و ستاره (*) استفاده نکن.")


def parse_analysis(text):
    """متن Claude را به {کلید: متن} تبدیل می‌کند؛ هر چیز قبل از اولین ### نادیده گرفته می‌شود."""
    parts = re.split(r"(?m)^\s*###\s*([A-Za-z_]+)\s*$", text)
    return {parts[i].lower(): parts[i + 1].strip() for i in range(1, len(parts) - 1, 2) if parts[i + 1].strip()}


def fix_rtl_lines(text):
    return "\n".join(fix_rtl(l) for l in text.split("\n"))


def format_analysis(sections, loc, cfg):
    """بلوک‌ها را به پیام‌های کمتر از TG_MAX کاراکتر بسته‌بندی می‌کند (HTML تلگرام، راست‌به‌چپ)."""
    head = f"{RLM}📅 <b>تحلیل فاندامنتال بازار</b> | {loc.strftime('%Y-%m-%d')} ساعت {loc.strftime('%H:%M')}"
    blocks = [head]
    for key, emoji, name, _w in ANALYSIS_ASSETS:
        if key in sections:
            blocks.append(f"{RLM}{emoji} <b>{esc(name)}</b>\n{esc(fix_rtl_lines(sections[key]))}")
    if "summary" in sections:
        blocks.append(f"{RLM}🧭 <b>جمع‌بندی</b>\n{esc(fix_rtl_lines(sections['summary']))}")
    blocks.append(f"{RLM}⚠️ این تحلیل خودکار و بر پایهٔ خبرهای ۲۴ ساعت اخیر است و توصیهٔ خرید یا فروش نیست.")
    msgs, cur = [], ""
    for b in blocks:
        b = b[:TG_MAX]
        if cur and len(cur) + 2 + len(b) > TG_MAX:
            msgs.append(cur)
            cur = b
        else:
            cur = f"{cur}\n\n{b}" if cur else b
    if cur:
        msgs.append(cur)
    return msgs


def run_analysis(cfg, post_fn, loc, get_feed=None, ask_fn=None, now=None):
    """تحلیل را می‌سازد و می‌فرستد. خروجی: تعداد پیام‌های ارسال‌شده. در خطای ساخت/ارسال پیام اول، استثنا می‌دهد."""
    now = now or datetime.now(timezone.utc)
    ask_fn = ask_fn or (lambda system, user: claude_text(
        cfg, system, user, model=cfg.get("analysis_model") or cfg["ai_model"],
        web_search=bool(cfg.get("analysis_web_search"))))
    heads = collect_headlines(cfg, get_feed, now=now)
    log(f"تحلیل روزانه: {len(heads)} تیتر ۲۴ ساعت اخیر جمع شد.")
    text = ask_fn(analysis_system(cfg), build_analysis_prompt(cfg, heads, loc, now))
    sections = parse_analysis(text)
    if len([k for k in sections if k != "summary"]) < 3:
        raise RuntimeError("پاسخ Claude قالب درست نداشت (کمتر از ۳ دارایی تحلیل شده بود).")
    sent = 0
    for m in format_analysis(sections, loc, cfg):
        try:
            post_fn(m)
        except Exception as e:     # noqa
            if sent == 0:
                raise
            log(f"⚠ بخشی از تحلیل ارسال نشد: {str(e)[:100]}")
            break
        sent += 1
        time.sleep(2)
    return sent


def maybe_analysis(cfg, post_fn, save_fn=None, force=False, get_feed=None, ask_fn=None, now=None):
    """اگر نوبت تحلیل روزانه رسیده بود (و قبلاً ارسال نشده)، آن را می‌فرستد. خروجی: تعداد پیام‌ها."""
    if not force and not cfg.get("analysis_enabled", True):
        return 0
    sent_list = _STATE_EXTRA["analysis_sent"]
    slot, loc = due_analysis_slot(cfg, sent_list, now)
    if force:
        slot = loc.strftime("%Y-%m-%d %H:%M") + " manual"
    if not slot:
        return 0
    if not cfg.get("anthropic_key"):
        if "analysis_nokey" not in _warned:
            log("⚠ تحلیل روزانه نیاز به کلید Anthropic دارد (ANTHROPIC_API_KEY)؛ فعلاً ارسال نمی‌شود.")
            _warned.add("analysis_nokey")
        return 0
    if _analysis_fails.get(slot, 0) >= 3:
        return 0               # چند بار ناموفق بود؛ برای همین نوبت دیگر هزینه نمی‌کنیم
    try:
        n = run_analysis(cfg, post_fn, loc, get_feed=get_feed, ask_fn=ask_fn, now=now)
    except Exception as e:     # noqa
        _analysis_fails[slot] = _analysis_fails.get(slot, 0) + 1
        log(f"تحلیل روزانه ناموفق ({_analysis_fails[slot]}/3): {str(e)[:140]}")
        return 0
    if n and not force:
        sent_list.append(slot)
        del sent_list[:-30]
        if save_fn:
            save_fn()
    log(f"تحلیل روزانه ارسال شد ({n} پیام).")
    return n


# ------------------------- اجرا -------------------------
def main():
    global _LOG_FILE, _LOG_MAX
    ap = argparse.ArgumentParser()
    ap.add_argument("--test", action="store_true", help="ارسال یک پیام تست به کانال")
    ap.add_argument("--dry-run", action="store_true", help="فقط چاپ در ترمینال (ارسال نمی‌کند)")
    ap.add_argument("--backfill", type=int, default=0, help="در اولین اجرا N خبر آخر را هم منتشر کن")
    ap.add_argument("--setup", action="store_true", help="اجرای دوبارهٔ جادوگر راه‌اندازی (توکن/کانال)")
    ap.add_argument("--once", action="store_true", help="فقط یک دور بررسی و خروج (برای cron)")
    ap.add_argument("--test-ai", action="store_true", help="تست کلید و نام مدل Anthropic")
    ap.add_argument("--analysis-now", action="store_true",
                    help="تحلیل فاندامنتال را همین الان بساز و بفرست (با --dry-run فقط چاپ می‌کند)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        return selftest()
    if a.setup:
        sys.exit(0 if wizard() else 1)
    cfg = load_cfg()
    if cfg.get("log_to_file"):
        _LOG_FILE = os.path.join(BASE, "news_agent.log")
        _LOG_MAX = int(cfg.get("log_max_bytes", 1000000))

    if a.test_ai:
        if not cfg.get("anthropic_key"):
            sys.exit("کلید Anthropic تنظیم نشده است. ANTHROPIC_API_KEY را ست کن یا در news_config.json بگذار.")
        try:
            out = ai_summary(cfg, "Gold rises as CPI cools",
                             "Spot gold rose 1% on Tuesday after US consumer prices cooled more than expected.", "test")
        except Exception as e:
            sys.exit(f"❌ تست ناموفق: {e}\n   اگر خطا 404 است، نام ai_model را بررسی کن (پیش‌فرض: {DEFAULTS['ai_model']}).")
        print("✅ مدل پاسخ داد:", out or "(NONE)")

    if a.analysis_now:
        if not cfg.get("anthropic_key"):
            sys.exit("تحلیل به کلید Anthropic نیاز دارد. ANTHROPIC_API_KEY را ست کن یا در news_config.json بگذار.")
        if a.dry_run:
            post_a = lambda m: print("-" * 50 + "\n" + re.sub(r"<[^>]+>", "", html.unescape(m)))  # noqa
        elif not cfg["discord_webhook"] and (not cfg["token"] or not cfg["channel"]):
            sys.exit("token یا channel خالی است (برای فقط چاپ، --dry-run هم بگذار).")
        else:
            post_a = make_post(cfg)
        n = maybe_analysis(cfg, post_a, force=True)
        sys.exit(0 if n else 1)

    if a.dry_run:
        def post(m):
            print("-" * 50 + "\n" + re.sub(r"<[^>]+>", "", html.unescape(m)))
        seen, recent = load_state()
        # بدون فایل وضعیت، پیش‌نمایش باید چند خبر نشان دهد (نه اینکه همه را فقط علامت بزند)
        bf = a.backfill or (0 if seen is not None else cfg["max_per_cycle"])
        first = seen is None
        seen, recent = seen or [], recent or []
        n = run_cycle(cfg, seen, post, first=first, backfill=bf, recent=recent)
        log(f"پایان پیش‌نمایش: {n} خبر.")
        return
    if not cfg["discord_webhook"] and (not cfg["token"] or not cfg["channel"]):
        sys.exit(f"token یا channel خالی است. فایل را ویرایش کن:\n  {CFG_FILE}")
    if not cfg["discord_webhook"] and looks_like_bot(cfg["channel"]):
        sys.exit("مقدار channel یوزرنیم یک ربات است (آخرش bot دارد). یوزرنیم کانال را بگذار، یا  python news_agent.py --setup  را اجرا کن.")
    if a.test:
        try:
            make_post(cfg)("✅ ربات خبری بازار با موفقیت به کانال وصل شد.")
        except RuntimeError as e:
            sys.exit(f"{e}\nراهنما: {explain_tg_error(str(e)) or 'python news_agent.py --setup را اجرا کن.'}")
        log("پیام تست ارسال شد؛ کانال را ببین.")
        return

    lock = acquire_lock()          # دو نمونهٔ هم‌زمان = پست تکراری؛ جلوگیری می‌کنیم
    seen, recent = load_state()
    first = seen is None
    if a.backfill > 0:
        if not first:
            log("--backfill: فهرست خبرهای دیده‌شدهٔ قبلی نادیده گرفته شد؛ ممکن است چند خبر اخیر تکراری شوند.")
        seen, recent, first = [], [], True
    seen = seen or []
    recent = recent or []
    post = make_post(cfg)
    save = lambda: save_state(seen, recent)  # noqa

    def _on_term(signum, frame):
        raise KeyboardInterrupt
    try:
        signal.signal(signal.SIGTERM, _on_term)      # systemd/docker: استیت قبل از خروج ذخیره شود
    except (ValueError, AttributeError):
        pass

    if a.once:
        try:
            n = run_cycle(cfg, seen, post, first=first, backfill=a.backfill, recent=recent, save_fn=save)
            save_state(seen, recent)
            log(f"{n} خبر ارسال شد.")
            maybe_analysis(cfg, post, save_fn=save)      # تحلیل روزانه (فقط اگر نوبتش رسیده باشد)
            save_state(seen, recent)
        except KeyboardInterrupt:
            save_state(seen, recent)
        except Exception as e:  # noqa
            save_state(seen, recent)
            sys.exit(f"خطا: {e}  {explain_tg_error(str(e))}")
        return

    log(f"ایجنت خبری شروع شد | هر {cfg['poll_seconds']} ثانیه | Ctrl+C = توقف")
    if first and not a.backfill:
        log("اولین اجرا: خبرهای فعلی فقط علامت‌گذاری می‌شوند و از خبرهای جدید شروع می‌کنم.")
    while True:
        try:
            n = run_cycle(cfg, seen, post, first=first, backfill=a.backfill, recent=recent, save_fn=save)
            first = False
            save_state(seen, recent)
            if n:
                log(f"{n} خبر ارسال شد.")
            maybe_analysis(cfg, post, save_fn=save)      # تحلیل روزانه (فقط اگر نوبتش رسیده باشد)
        except KeyboardInterrupt:
            save_state(seen, recent)
            log("متوقف شد.")
            return
        except Exception as e:  # noqa
            log(f"خطا: {e}  {explain_tg_error(str(e))}")
        try:
            time.sleep(cfg["poll_seconds"])
        except KeyboardInterrupt:
            save_state(seen, recent)
            log("متوقف شد.")
            return


# ------------------------- تست آفلاین -------------------------
SAMPLE = """<?xml version="1.0"?><rss version="2.0"><channel>
<item><title>Gold jumps as soft CPI lifts rate-cut bets</title><link>https://ex.com/1</link><guid>g1</guid><pubDate>{now}</pubDate><description><![CDATA[<p>Spot gold rose 1% on Tuesday after US consumer prices cooled more than expected. Traders now see a smaller chance of a December hike. The dollar weakened.</p>]]></description></item>
<item><title>Nvidia and Nasdaq slide as AI stocks tumble</title><link>https://ex.com/2</link><guid>g2</guid><pubDate>{now}</pubDate></item>
<item><title>Local bakery wins award</title><link>https://ex.com/3</link><guid>g3</guid><pubDate>{now}</pubDate></item>
<item><title>Euro slips ahead of ECB speakers</title><link>https://ex.com/4</link><guid>g4</guid><pubDate>{now}</pubDate></item>
<item><title>Old silver story</title><link>https://ex.com/5</link><guid>g5</guid><pubDate>Mon, 01 Jan 2024 10:00:00 GMT</pubDate></item>
<item><title>Gold jumps as soft CPI lifts rate-cut bets</title><link>https://ex.com/dup</link><guid>g6</guid><pubDate>{now}</pubDate></item>
</channel></rss>"""
ATOM = """<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Dollar index rises &amp; Treasury yields climb</title>
<link href="https://ex.com/a1"/><id>a1</id><updated>{iso}</updated></entry></feed>"""


def selftest():
    global translate_fa
    from email.utils import format_datetime
    now = datetime.now(timezone.utc)
    sample = SAMPLE.format(now=format_datetime(now)).encode()
    atom = ATOM.format(iso=now.isoformat()).encode()
    assert len(parse_feed(sample)) == 6 and parse_feed(atom)[0]["link"] == "https://ex.com/a1"
    c, h = classify("Gold jumps as soft CPI lifts rate-cut bets")
    assert "gold" in c and "macro" in c and h
    assert classify("Local bakery wins award")[0] == []
    assert "btc" in classify("Bitcoin slides below $90,000 as ETF outflows grow")[0]
    sm = (b'<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" '
          b'xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"><url><loc>https://ex.com/s1</loc><news:news>'
          b'<news:title>Gold hits record</news:title><news:publication_date>2026-06-16T02:10:34.0000000Z'
          b'</news:publication_date></news:news></url></urlset>')
    r = parse_feed(sm)
    assert r and r[0]["title"] == "Gold hits record" and r[0]["link"] == "https://ex.com/s1" and r[0]["date"]
    assert "dow" in classify("Dow Jones closes higher")[0] and "dowstk" in classify("Microsoft earnings beat")[0]
    assert classify("Hurry! Goldman Sachs says shoulder pain")[0].count("gold") == 0   # 'Goldman' ≠ gold
    for bad_t in ("Visa waiver extended for holiday travelers", "James Bond actor named",
                  "Pound of beef prices rise at Dollar General", "Gold medal for Norway at Winter Games",
                  "Apple pie recipe wins award"):
        assert classify(bad_t)[0] == [], bad_t
    assert "dowstk" in classify("Apple shares jump after earnings beat")[0]
    mt = (b'<rss xmlns:media="http://search.yahoo.com/mrss/"><channel><item><title>Gold jumps</title>'
          b'<media:title>Cap</media:title><link>http://a</link></item></channel></rss>')
    assert parse_feed(mt)[0]["title"] == "Gold jumps"

    # --- جهت خبر: صعودی و نزولی با تیتر تقریباً یکسان نباید «تکراری» شوند ---
    assert direction("Dollar rises after Fed decision") == 1
    assert direction("Dollar falls after Fed decision") == -1
    assert direction("Gold steady ahead of CPI") == 0
    up = title_tokens("Dollar rises after Fed decision")
    dn = title_tokens("Dollar falls after Fed decision")
    assert up & {_UP_TOKEN} and dn & {_DOWN_TOKEN}
    assert not is_similar(up, dn, 0.6), "خبر صعودی و نزولی نباید تکراری حساب شود"
    assert is_similar(title_tokens("Gold jumps as CPI cools"), title_tokens("Gold jumps as CPI cools today"), 0.6)
    assert not is_similar(up, title_tokens("Microsoft earnings beat estimates"), 0.6)

    # --- تحلیل روزانه: نوبت‌ها، جمع‌آوری تیتر، قالب‌بندی، ارسال یکبار در هر نوبت ---
    acfg = dict(DEFAULTS, analysis_enabled=True, anthropic_key="k", ai_model="m")
    utc = lambda h, m: datetime(2026, 11, 20, h, m, tzinfo=timezone.utc)      # تهران = UTC+3:30
    assert due_analysis_slot(acfg, [], utc(6, 30))[0] == "2026-11-20 10:00"    # ۱۰:۰۰ تهران
    assert due_analysis_slot(acfg, [], utc(6, 29))[0] is None                  # یک دقیقه زود
    assert due_analysis_slot(acfg, [], utc(7, 59))[0] == "2026-11-20 10:00"    # هنوز در مهلت ۹۰ دقیقه
    assert due_analysis_slot(acfg, [], utc(8, 0))[0] is None                   # مهلت تمام شد
    assert due_analysis_slot(acfg, [], utc(11, 30))[0] == "2026-11-20 15:00"   # ۱۵:۰۰ تهران
    assert due_analysis_slot(acfg, ["2026-11-20 10:00"], utc(6, 40))[0] is None  # قبلاً ارسال شده
    heads_now = datetime.now(timezone.utc)
    titles = ["Gold climbs as Fed rate-cut bets build", "Silver rallies with gold", "Nasdaq slips as chip stocks fall",
              "Dow Jones closes higher on Wall Street", "Oil jumps on OPEC+ supply cut", "Bitcoin drops below $90,000",
              "Dollar index steadies before CPI", "Pound sterling slips after Bank of England signals cuts",
              "Canadian dollar edges up as Bank of Canada holds rates", "Old gold story from last week"]
    fake_items = [{"title": t, "date": heads_now - timedelta(hours=(200 if "Old" in t else 2)), "desc": "", "link": ""}
                  for t in titles]
    heads = collect_headlines(acfg, get_feed=lambda url: list(fake_items), now=heads_now)
    assert len(heads) == len(titles) - 1 and all("Old" not in h["title"] for h in heads)      # تکراری فیدها یکی شد، خبر قدیمی حذف
    prompt = build_analysis_prompt(acfg, heads, tz_now("Asia/Tehran", utc(6, 30)), heads_now)
    assert "=== cad ===" in prompt and "Canadian dollar edges up" in prompt and "Old gold story" not in prompt
    fake_ai = "\n".join(f"### {k}\nتحلیل نمونهٔ {k}؛ Fed و تورم مهم‌اند & سوگیری خنثی." for k, *_ in ANALYSIS_ASSETS) + "\n### summary\nجمع‌بندی نمونه."
    secs = parse_analysis("مقدمهٔ اضافه\n" + fake_ai)
    assert set(secs) == {k for k, *_ in ANALYSIS_ASSETS} | {"summary"}
    msgs = format_analysis(secs, tz_now("Asia/Tehran", utc(6, 30)), acfg)
    assert msgs and all(len(m) <= 4096 for m in msgs) and "&amp;" in msgs[0] and "دلار کانادا" in "".join(msgs)
    assert "&amp<" not in "".join(msgs).replace(RLM, "<")       # نویسهٔ RLM وسط موجودیت HTML نیفتاده
    long_secs = {k: "کلمه " * 300 for k, *_ in ANALYSIS_ASSETS}
    assert all(len(m) <= 4096 for m in format_analysis(long_secs, tz_now("Asia/Tehran", utc(6, 30)), acfg))
    posted, calls = [], []
    _STATE_EXTRA["analysis_sent"] = []
    ask = lambda system, user: (calls.append(1), fake_ai)[1]
    n1 = maybe_analysis(acfg, posted.append, save_fn=lambda: None, get_feed=lambda u: list(fake_items), ask_fn=ask, now=utc(6, 40))
    n2 = maybe_analysis(acfg, posted.append, save_fn=lambda: None, get_feed=lambda u: list(fake_items), ask_fn=ask, now=utc(6, 50))
    assert n1 >= 1 and n2 == 0 and len(calls) == 1, (n1, n2, calls)      # در هر نوبت فقط یک‌بار
    assert _STATE_EXTRA["analysis_sent"] == ["2026-11-20 10:00"]
    st = os.path.join(tempfile.mkdtemp(), "st.json")
    save_state([], [], st)
    _STATE_EXTRA["analysis_sent"] = []
    load_state(st)
    assert _STATE_EXTRA["analysis_sent"] == ["2026-11-20 10:00"]            # در فایل وضعیت ذخیره و برگردانده می‌شود
    assert maybe_analysis(dict(acfg, anthropic_key=""), posted.append, get_feed=lambda u: [], ask_fn=ask, now=utc(11, 40)) == 0
    assert maybe_analysis(dict(acfg, analysis_enabled=False), posted.append, get_feed=lambda u: [], ask_fn=ask, now=utc(11, 40)) == 0
    bad = lambda system, user: "فقط یک متن بدون قالب"
    assert maybe_analysis(acfg, posted.append, get_feed=lambda u: [], ask_fn=bad, now=utc(11, 40)) == 0     # قالب خراب = ارسال نمی‌شود
    assert "2026-11-20 15:00" not in _STATE_EXTRA["analysis_sent"]                                       # و علامت «ارسال‌شده» نمی‌خورد
    _STATE_EXTRA["analysis_sent"] = []

    # --- ساعت به وقت تهران، مستقل از ساعت سیستم (مثلاً UTC در گیت‌هاب) ---
    assert local_time(datetime(2026, 11, 19, 14, 30, tzinfo=timezone.utc), "Asia/Tehran") == "18:00"

    # --- راست‌به‌چپ: خط فارسیِ شروع‌شده با کلمهٔ انگلیسی باید با RLM شروع شود ---
    t = fix_rtl("TD Cowen مجدداً خرید سهام والمارت را تکرار می‌کند و قیمت هدف 125 دلار را حفظ می‌کند")
    assert t.startswith(RLM) and "TD Cowen" + RLM in t and "125" + RLM in t and t.count(RLM) == 3, repr(t)
    assert fix_rtl("Only English headline") == "Only English headline"
    assert fix_rtl(fix_rtl("TD Cowen والمارت را خرید")) == fix_rtl("TD Cowen والمارت را خرید")      # idempotent

    # --- استیت: رفت‌وبرگشت و سازگاری با فرمت قدیمی ---
    with tempfile.TemporaryDirectory() as td:
        p = os.path.join(td, "s.json")
        save_state(["a", "b"], [title_tokens("Gold rises now")], path=p)
        s2, r2 = load_state(p)
        assert s2 == ["a", "b"] and r2 and _UP_TOKEN in r2[0]
        with open(p, "w", encoding="utf-8") as f:
            json.dump(["old1", "old2"], f)          # فرمت قدیمی
        s3, r3 = load_state(p)
        assert s3 == ["old1", "old2"] and r3 == []
        with open(p, "w", encoding="utf-8") as f:
            f.write("{broken")
        s4, r4 = load_state(p)
        assert s4 is None and r4 == [] and os.path.exists(p + ".corrupt")

    cfg = dict(DEFAULTS, max_age_minutes=60, show_original=True, fetch_article=False)
    sent, seen = [], []
    real_tr, real_sleep = translate_fa, time.sleep
    translate_fa = lambda t: "ترجمه: " + t   # noqa
    time.sleep = lambda s: None
    feeds = {u: (sample if i == 0 else atom if i == 1 else None) for i, (_, u) in enumerate(FEEDS)}

    def gf(url):
        if feeds[url] is None:
            raise RuntimeError("down")
        return parse_feed(feeds[url])
    try:
        n = run_cycle(cfg, seen, sent.append, get_feed=gf, first=True)
        assert n == 0 and sent == [] and seen                      # اولین اجرا: فقط علامت‌گذاری
        n = run_cycle(cfg, seen, sent.append, get_feed=gf)
        assert n == 0                                              # چیز جدیدی نیست
        seen2 = []
        n = run_cycle(cfg, seen2, sent.append, get_feed=gf)
        txt = "\n".join(sent)
        assert n == 4, n                                           # gold(+dup حذف), nvidia, euro, dollar
        assert "ترجمه: Gold jumps" in txt and "🔴 مهم" in txt and "Local bakery" not in txt and "Old silver" not in txt
        assert txt.count("Gold jumps") == 2                        # یک‌بار فارسی + یک‌بار اصلی داخل همان پیام
        assert "&amp;" in txt                                      # HTML امن
        assert "ترجمه: Spot gold rose 1% on Tuesday" in txt        # چکیدهٔ خبر ترجمه و منتشر می‌شود
        def boom(m):
            raise RuntimeError("boom")
        seen3 = []
        assert run_cycle(cfg, seen3, boom, get_feed=gf) == 0       # ارسال ناموفق = علامت نخورد
        assert run_cycle(cfg, seen3, sent.append, get_feed=gf) == 4  # دور بعد همه ارسال می‌شوند
        assert make_summary("Short.", "x", 300) == ""
        assert len(make_summary("A sentence here. " * 40, "x", 100)) <= 101
        assert _clip_text("x " * 2000)[-1] != " " and len(_clip_text("x " * 2000)) <= 1001
    finally:
        translate_fa, time.sleep = real_tr, real_sleep
    print("selftest OK")
    print(sent[0])


if __name__ == "__main__":
    main()
