# Apache SEO Hardening

Apply [`japantravel-seo-hardening.conf`](/root/japantravel/deploy/apache/japantravel-seo-hardening.conf) inside the active virtual host or Apache include path.

Expected effects:
- `/wp-content/uploads/` directory listing disabled
- `X-Robots-Tag: noindex, follow, nosnippet` on `/wp-includes/*.js` and `/wp-includes/*.css`

Typical rollout:

```bash
sudo cp deploy/apache/japantravel-seo-hardening.conf /etc/apache2/conf-available/japantravel-seo-hardening.conf
sudo a2enconf japantravel-seo-hardening
sudo systemctl reload apache2
```
