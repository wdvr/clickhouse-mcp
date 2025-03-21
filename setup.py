from setuptools import setup, find_packages

setup(
    name="clickhouse_mcp",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    package_data={
        "clickhouse_mcp": ["index/*"],
    },
    include_package_data=True,
    install_requires=[
        "requests>=2.25.0",
        "mcp>=1.3.0",
        "langchain-aws>=0.2.15",
        "langchain>=0.1.0",
        "langchain-community>=0.1.0",
        "clickhouse-connect>=0.6.0",
        "python-dotenv>=0.21.0",
        "faiss-cpu>=1.7.4",
    ],
    python_requires=">=3.7",
)
