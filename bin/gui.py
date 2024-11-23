import gradio as gr
import requests
import pandas as pd
import os 

# Define base URL for the FastAPI endpoints
BASE_URL = f"http://localhost:{os.environ['UVICORN_PORT']}"  # Adjust if the FastAPI server runs on a different host or port

# Functions to interact with the API
def list_tasks():
    response = requests.get(f"{BASE_URL}/tasks/")
    if response.status_code == 200:
        tasks = response.json()
        if isinstance(tasks, list) and tasks:
            # Convert the list of tasks to a DataFrame for table display
            df = pd.DataFrame(tasks)
            return df, [task['task_id'] for task in tasks if 'task_id' in task]
        else:
            return pd.DataFrame(columns=["No tasks available"]), []
    else:
        return pd.DataFrame([[f"Error: {response.status_code} - {response.text}"]], columns=["Error"]), []

def task_status(task_id):
    if not task_id:
        return "Please provide a Task ID."
    response = requests.get(f"{BASE_URL}/tasks/status/{task_id}")
    if response.status_code == 200:
        status_info = response.json()
        return status_info
    else:
        return f"Error: {response.status_code} - {response.text}"

def stop_task(task_id):
    if not task_id:
        return "Please provide a Task ID."
    response = requests.get(f"{BASE_URL}/tasks/stop/{task_id}")
    if response.status_code == 200:
        result = response.json()
        return result
    else:
        return f"Error: {response.status_code} - {response.text}"

def get_workers():
    response = requests.get(f"{BASE_URL}/workers/")
    if response.status_code == 200:
        workers_info = response.json()
        return workers_info
    else:
        return f"Error: {response.status_code} - {response.text}"

# Build Gradio Interface
with gr.Blocks() as demo:
    gr.Markdown("# Task Management GUI using Gradio")

    # Tasks Tab (combining listing, checking status, and stopping)
    with gr.Tab("Task Operations"):
        with gr.Column():
            list_tasks_btn = gr.Button("List All Tasks")
            task_list_output = gr.Dataframe(label="Tasks", headers=["task_id", "status", "result", "date_done"], interactive=False)
        task_id_dropdown = gr.Dropdown(label="Task ID", choices=[], interactive=True)

        # Button to list tasks and populate task IDs
        def fetch_and_display_tasks():
            tasks_df, task_ids = list_tasks()
            return tasks_df, gr.update(choices=task_ids)
        
        list_tasks_btn.click(fn=fetch_and_display_tasks, inputs=None, outputs=[task_list_output, task_id_dropdown])
        
        with gr.Row():
            check_status_btn = gr.Button("Check Task Status")
            stop_task_btn = gr.Button("Stop Task")
        
        task_status_output = gr.Textbox(label="Task Status Result", placeholder="Task status or operation result will appear here...")

        # Buttons for task status and stop operations
        check_status_btn.click(fn=task_status, inputs=task_id_dropdown, outputs=task_status_output)
        stop_task_btn.click(fn=stop_task, inputs=task_id_dropdown, outputs=task_status_output)

    # Workers Tab
    with gr.Tab("Workers"):
        get_workers_btn = gr.Button("Get Workers")
        workers_output = gr.Textbox(label="Workers", placeholder="Worker information will appear here...")
        get_workers_btn.click(fn=get_workers, inputs=None, outputs=workers_output)

# Launch the interface
demo.launch()
