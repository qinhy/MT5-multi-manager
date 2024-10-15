import pytest
from fastapi.testclient import TestClient
from tasks import RESTapi, MT5Account, Book

# Initialize TestClient with your FastAPI app
client = TestClient(RESTapi.api)

# Mock MT5Account and Book for testing
class MockMT5Account(MT5Account):
    # Define mock attributes as needed
    account_id = "mock_account_id"

class MockBook(Book):
    # Define mock attributes as needed
    symbol = "EURUSD"
    price_open = 1.2345
    volume = 0.1

@pytest.fixture
def mock_account():
    return MockMT5Account()

@pytest.fixture
def mock_book():
    return MockBook()

def test_add_terminal():
    response = client.post("/terminals/add", json={"broker": "mock_broker", "path": "/mock/path"})
    assert response.status_code == 200
    assert response.json() == {"status": "Terminal added"}

def test_get_books(mock_account):
    response = client.post("/books/", json=mock_account.dict())
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_account_info(mock_account):
    response = client.post("/account/info", json=mock_account.dict())
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_book_send(mock_account, mock_book):
    response = client.post("/books/send", json={"acc": mock_account.dict(), "book": mock_book.dict()})
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_book_close(mock_account, mock_book):
    response = client.post("/books/close", json={"acc": mock_account.dict(), "book": mock_book.dict()})
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_book_change_price(mock_account, mock_book):
    response = client.post("/books/change-price", json={"acc": mock_account.dict(), "book": mock_book.dict(), "p": 1.2500})
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_book_change_trailing_stop(mock_account, mock_book):
    response = client.post("/books/change-trailing-stop", json={"acc": mock_account.dict(), "book": mock_book.dict(), "tp": 1.2600, "sl": 1.2200})
    assert response.status_code == 200
    assert "task_id" in response.json()

def test_task_status():
    # Assuming you have an existing task_id for testing
    task_id = "mock_task_id"
    response = client.get(f"/tasks/status/{task_id}")
    assert response.status_code == 200
    assert "task_id" in response.json()
    assert "status" in response.json()
    assert "result" in response.json()
