# Dasein Trading Core
- a part of dasein-trainding-systems
- contains a lot of primitives to construct trading bots, trading systems, signals, analytic tools and so on.
- was properly used for many experiments, and real-time working trading systems
- was private, turned to public
- is it free and any can use it for his own needs
- if you want collaboration or help with trading feel free to contact author acidpictures@gmail.com, telegram: @acidpictures
- can contain some bugs, open issue and maybe authore have some time to fix that





## Venv
```bash
source venv/bin/activate

```
## Lint

To lint your code using flake8, just run in your terminal:

```bash
$ make test.lint
```

It will run the flake8 commands on your project in your server container, and display any lint error you may have in your code.

## Format

The code is formatted using [Black](https://github.com/python/black) and [Isort](https://pypi.org/project/isort/). You have the following commands to your disposal:

```bash
$ make format.black # Apply Black on every file
$ make format.isort # Apply Isort on every file
```

