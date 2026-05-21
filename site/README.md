# LionClaw Site

The site is a small Zola project.

```sh
zola --root site serve
zola --root site build
```

Static files live in `site/static/` and are copied into the generated site as-is.
That includes `site/static/.htaccess`.

If you use a plain static server, serve `site/public/` after `zola --root site build`.
Do not serve the `site/` source directory directly.
