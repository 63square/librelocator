# LibreLocator

UK postcode locator, inspired by github.com/lexbailey/nearmypostcode

## Differences

NearMyPostcode (NMP) uses a 5MB data bundle which I find unacceptable for wide usage, NMP also promotes serving the bundle over the webserver which is not ideal as it can be served by a CDN to provide faster downloads or eliminate the download step completely if cached locally by another site.

LibreLocator intends to work on these issues by providing the smallest bundle possible by sacrificing read time over size.