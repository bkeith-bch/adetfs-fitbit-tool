import setuptools

with open("README.md","r",encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ADETfs-herissonbleu"
    version="1.0.rc1"
    author="herissonbleu"
    author_email="herisson_bleu@outlook.com"
    description="Automated Data Extraction Tool for Fitbit Server"
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: BSD 3-Clause",
        "Operating System :: OS Independent"
    ]
    package_dir={"":"src"},
    packages=setuptools.find_packages(where="src")
    python_requires=">=3.8"
)