## slides/index.html content: Copyright (C) 2022, Jonathan Woodring

## slides/ framework: <https://revealjs.com/>

`slides/LICENSE`

Copyright (C) 2011-2022 Hakim El Hattab, http://hakim.se, and reveal.js
contributors

## isolated galaxy data set credits

isolated\_galaxy from yt project at <https://yt-project.org/>

<https://yt-project.org/data/IsolatedGalaxy.tar.g>

## sqlitehelpers.py, SQLiteReader.py, tutorial.ipynb

sqlitehelpers.py: CC0 "No rights reserved" - feel free to use anywhere
SQLite3Reader.py: CC0 "No rights reserved" - feel free to use anywhere
tutorial.ipynb: CC0 "No rights reserved" - feel free to use anywhere

## running slides

open `slides/index.html` or run `slides.sh` if on a posix system

## to use the Jupyter notebook

Install jupyter notebook or install it through Anaconda Python:

```
$ conda create -n tutorial python=3.10
$ conda activate tutorial
$ conda install matplotlib numpy jupyter scipy
```

run "jupyter notebook" and open tutorial.ipynb in your browser

## using the ParaView SQLite3Reader.py

You need a sufficiently recent version of ParaView (I used 5.9.1) that can
load ParaView plugins

In the ParaView menu, go to "Tools -> Manage Plugins"

In the dialogue, pick "Load New" and select "SQLite3Reader.py"

Now, you can open SQLite3 databases with the `.sqlite` or `.sqlite3` extension
