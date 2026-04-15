# WordPress SEO Hardening Plugin

Copy [`japantravel-seo-hardening.php`](/root/japantravel/deploy/wordpress/japantravel-seo-hardening/japantravel-seo-hardening.php) into `wp-content/plugins/japantravel-seo-hardening/` and activate it.

The plugin enforces:
- `robots.txt` sitemap line
- `noindex,follow` on category, tag, author, date, search, attachment archives
- `X-Robots-Tag: noindex, follow` on feed responses
- Rank Math taxonomy sitemap exclusion for `category` and `post_tag`
- feed discovery link removal from `<head>`
