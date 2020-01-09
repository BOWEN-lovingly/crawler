from setuptools import setup, find_packages
setup(
    name = 'crawler',
    version = '0.1.0',
    url = '',
    description = '',
    packages = find_packages(),
    install_requires = [
        # Github Private Repository - needs entry in `dependency_links`
        'BeautifulSoup'
    ],
    dependency_links = [
     "https://github.com/BOWEN-lovingly/crawler.git"
    ]
)
