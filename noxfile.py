import nox


@nox.session
def test(session):
    session.install("coverage", "pytest", "pytest-asyncio", "flit")
    session.install("uvicorn", "httpx")
    session.run("flit", "install")
    session.run("coverage", "erase")
    session.run("coverage", "run", "--include=drains/*", "-m", "pytest", "-ra")
    session.run("coverage", "report", "-m")


@nox.session
def lint(session):
    session.install("black", "isort")
    session.run("black", ".")
    session.run("isort", ".")
