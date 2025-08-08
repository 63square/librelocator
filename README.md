# LibreLocator (WIP)

UK postcode locator, inspired by github.com/lexbailey/nearmypostcode

This project is in early development, expect bugs and poor performance.

## Why not NearMyPostcode (NMP)

NPM uses a 5MB data bundle which I find unacceptable for wide usage, also promoting serving the bundle over the webserver, which may be good for privacy but is certainly not ideal for performance. A better alternative which I suggest is serving the bundle over a CDN to provide faster downloads or even eliminate the download step completely.

LibreLocator intends to work on these issues by providing the smallest bundle possible, sacrificing read time over size if needed.