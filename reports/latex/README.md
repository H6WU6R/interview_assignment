# LaTeX Report

This folder contains the LaTeX source for the final project report.
`report.tex` contains the document preamble and imports each body section from `sections/`.

Build from this directory:

```bash
latexmk -pdf -interaction=nonstopmode report.tex
```

Clean generated TeX intermediates:

```bash
latexmk -C report.tex
```

The report uses the local `arxiv.sty` file from `kourgeorge/arxiv-style`.
`report.bbl` is kept after builds so the bibliography is reproducible even if BibTeX is not rerun.
