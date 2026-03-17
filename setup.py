from setuptools import setup, find_packages

setup(
    name="ai-secretary",
    version="0.1.0",
    description="AI Secretary - Outlook and Teams Analysis Tools",
    author="Jiatong Wang",
    packages=find_packages(),
    install_requires=[
        "requests>=2.31.0",
        "msal[broker]>=1.24.0",
        "PyJWT>=2.8.0",
        "playwright>=1.55.0",
        "openai>=1.0.0",
        "azure-identity>=1.15.0",
        "flask>=3.0.0",
    ],
    python_requires=">=3.8",
)
