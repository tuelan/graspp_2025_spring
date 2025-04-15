class SimpleOperationPerformer:
    """
    This is a simple class to demonstrate basic operations in Python.
    It's designed to be easy for beginners to understand.
    """
    def perform_string_print(self, input_string):
        """
        This action takes a piece of text (a 'string') and returns it.
        It also prints the result to the screen.
        """
        print("Running perform_string_print...")
        result = input_string
        print(f"The result of string_print function: {result}")
        return result

    def perform_addition(self, num1, num2, num3):
        """
        This action takes three numbers as input, adds them together,
        and returns the final sum. It also prints the result.
        """
        print("Running perform_addition...")
        final = num1 + num2 + num3
        print(f"The result of add_func function: {final}")
        return final

    def run_all_operations(self):
        """
        This puts all the actions together in order.
        First, it performs the string printing, then the addition.
        """
        print("\nStarting run_all_operations...")
        self.perform_string_print('LOOK MOM I CAN PRINT')
        self.perform_addition(5, 3, 2)
        print("Finished run_all_operations.")

# Let's create our SimpleOperationPerformer tool
operations_tool = SimpleOperationPerformer()

# Now, let's tell it to run each action individually
print("\nRunning operations individually:")
operations_tool.perform_string_print('Hello from individual call!')
operations_tool.perform_addition(10, 20, 30)

# And let's also run all operations together
operations_tool.run_all_operations()