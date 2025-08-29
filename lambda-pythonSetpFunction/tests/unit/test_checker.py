from functions.CheckBackupStatus import app


def test_CheckBackupStatus():
    data = app.lambda_handler(None, "")
    assert 0 <= data["stock_price"] > 0 <= 100
