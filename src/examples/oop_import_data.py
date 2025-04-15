import os
import pandas as pd
import logging


# Get the directory of the current script (notebook in this case)
current_script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Current python folder : {current_script_dir}")

# Navigate up two levels to reach the root directory
root_dir = os.path.dirname(os.path.dirname(current_script_dir))
print(f"Folder where all our files are located (base/root folder): {root_dir}")

# Construct the path to the data folder
data_folder = os.path.join(root_dir, "data")
print(f"Data folder: {data_folder}")

class SimpleFileDataProcessor:
    """
    This is a simple class to handle basic file operations.
    It's designed to be easy for beginners to understand.
    """
    def __init__(self, file_location="data/examples/module_1/"):
        """
        This is like the 'constructor' of our tool.
        It gets things ready when we create a SimpleFileDataProcessor.
        We set the location where our files are.
        """
        self.file_location = file_location
        self.data = None # We'll store our data here later

    def show_current_location(self):
        """
        This part shows where this Python program is currently running from.
        It's like saying 'I am here!'.
        """
        print('This file is run from the following path:')
        print("\n")
        print(os.getcwd())

    def list_files_in_folder(self):
        """
        This part looks inside a specific folder and tells us what files are there.
        Think of it as looking inside a box.
        """
        print("\n")
        print(f"Looking inside this folder: {self.file_location}")
        print(os.listdir(self.file_location))
        print()

    def load_data_from_csv(self, filename="world_bank_data.csv"):
        """
        This part reads data from a CSV file (like an Excel sheet) and puts it
        into a format that Python can easily work with. We call this a 'DataFrame'.
        """
        filepath = os.path.join(self.file_location, filename)
        try:
            self.data = pd.read_csv(filepath)
            print('Look mom this function imports data to pandas')
            print(self.data.head(5))
        except FileNotFoundError:
            print(f"Error: Could not find the file at {filepath}")
            self.data = None

    def run_all_steps(self):
        """
        This is the main part that puts everything together.
        It tells our tool to do all the steps in order.
        """
        print("Starting the file processing...")
        self.show_current_location()
        self.list_files_in_folder()
        self.load_data_from_csv()
        print("Finished processing.")

# Let's create our SimpleFileDataProcessor tool
processor = SimpleFileDataProcessor(
        file_location="data/examples/module_1/week_1"
)

# Now, let's tell it to do its job!
processor.run_all_steps()