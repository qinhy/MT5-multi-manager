import time
import requests

from Manager import Book

# Define the base URL of your API
BASE_URL = "http://localhost:8000"  # Replace with your actual API URL

# Helper function to raise error with custom message
def raise_error(response, expected_status):
    if response.status_code != expected_status:
        raise AssertionError(f"Expected status code {expected_status}, got {response.status_code}: {response.text}")

# Test adding a terminal
def test_add_terminal(broker, path):
    response = requests.post(f"{BASE_URL}/terminals/add", params={"broker": broker, "path": path})
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_add_terminal passed")

# Test listing all terminals
def test_list_terminals():
    response = requests.get(f"{BASE_URL}/terminals/")
    time.sleep(1)
    print(response.json())
    raise_error(response, 200)
    terminals = response.json()
    assert isinstance(terminals, dict), "Expected a dictionary of terminals"
    print("test_list_terminals passed")

# Test getting books for an account
def test_get_books(account):
    response = requests.post(f"{BASE_URL}/books/", json=account)
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_get_books passed")
    return res

# Test fetching account info
def test_account_info(account):
    response = requests.post(f"{BASE_URL}/account/info", json=account)
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_account_info passed")
    return res

# Test sending a book
def test_book_send(account, book):
    response = requests.post(f"{BASE_URL}/books/send", json={"acc": account, "book": book})
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_book_send passed")
    return res

# Test closing a book
def test_book_close(account, book):
    response = requests.post(f"{BASE_URL}/books/close", json={"acc": account, "book": book})
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_book_close passed")
    return res

# Test changing the price of a book
def test_book_change_price(account, book, price):
    response = requests.post(f"{BASE_URL}/books/change-price", json={"acc": account, "book": book, "p": price})
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_book_change_price passed")
    return res

# Test changing tp and sl values of a book
def test_book_change_tp_sl(account, book, tp, sl):
    response = requests.post(f"{BASE_URL}/books/change-tp-sl", json={"acc": account, "book": book, "tp": tp, "sl": sl})
    time.sleep(1)
    res = requests.get(f"http://localhost:8000/tasks/status/{response.json()['task_id']}").json()
    print(res)
    raise_error(response, 200)
    print("test_book_change_tp_sl passed")
    return res

# Run the tests with specific data
# if __name__ == "__main__":
#     # Example data for the tests
#     test_broker = "test_broker"
#     test_path = "/path/to/terminal"
#     test_account = {"id": 123456, "balance": 1000.0}
    # test_book = Book(symbol='',price_open = 100.0,volume= 0.01)
    
#     # Execute the tests with provided data
#     test_add_terminal(test_broker, test_path)
#     test_list_terminals()
#     test_get_books(test_account.model_dump())
#     test_account_info(test_account.model_dump())
    # test_book_send(test_account.model_dump(), test_book.model_dump())
    # test_book_close(test_account.model_dump(), test_book.model_dump())
    # test_book_change_price(test_account.model_dump(), test_book.model_dump(), 150.0)
    # test_book_change_tp_sl(test_account.model_dump(), test_book.model_dump(), 200.0, 50.0)
