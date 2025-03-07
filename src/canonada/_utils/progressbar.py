import math
import time
import sys


class ProgressBar:
    """
    CLI progress bar that handles both known and unknown total lengths.
    Shows elapsed time, remaining time, percentage, and processing rate.
    """

    def __init__(self, total:int|None=None, width:int=30, prefix:str="Progress:") -> None:
        """
        Initialize the progress bar.

        Args:
            total: Total number of items (if known)
            width: Width of the progress bar in characters
            prefix: Text to display before the progress bar
        """
        self.total = total if total is not None and total>0 else None
        self.width = width
        self.prefix = prefix
        self.start_time = time.time()
        self.current = 0
        self.last_output_length = 0

    def update(self, current:int|None=None) -> None:
        """
        Update the progress bar.

        Args:
            current: Current progress value (if None, increment by 1)
        """
        # Update current progress
        if current is not None:
            self.current = current
        else:
            self.current += 1

        # Calculate elapsed time
        current_time = time.time()
        elapsed_time = current_time - self.start_time
        elapsed_str = self._format_time(elapsed_time)

        if self.total is not None:
            # When total is known: show percentage and remaining time
            percent = min(100, self.current / self.total * 100)

            # Calculate remaining time
            if self.current > 0:
                items_per_second = self.current / elapsed_time
                remaining_items = self.total - self.current
                remaining_time = (
                    remaining_items / items_per_second if items_per_second > 0 else 0
                )
                remaining_str = self._format_time(remaining_time)
            else:
                remaining_str = "Unknown"

            # Calculate progress bar
            filled_length = int(self.width * self.current // self.total)
            bar = "█" * filled_length + "░" * (self.width - filled_length)

            # Create output string
            output = f"{self.prefix} |{bar}| {percent:.1f}% | {self.current}/{self.total} | Elapsed: {elapsed_str} | Remaining: {remaining_str}"
        else:
            # When total is unknown: show items processed and rate
            if self.current > 0:
                time_per_item = elapsed_time / self.current
                items_per_second = self.current / elapsed_time
                time_per_item_str = f"{self._format_time(time_per_item)}/item"
                speed_str = f"{items_per_second:.2f} items/s"
            else:
                time_per_item_str = "N/A"
                speed_str = "N/A"

            # For indeterminate progress, show a moving 'wave' in the progress bar
            bar_idx = int(
                (math.sin(self.current / self.width) / 2 + 0.5) * (self.width - 4)
            )
            bar = "░" * bar_idx + "█" * 5 + "░" * (self.width - 5 - bar_idx)

            # Create output string
            output = f"{self.prefix} |{bar}| Items: {self.current} | Elapsed: {elapsed_str} | {speed_str} | {time_per_item_str}"

        # Clear the current line and write the new output
        sys.stdout.write("\r" + " " * self.last_output_length + "\r" + output)
        sys.stdout.flush()

        # Save the length of the current output
        self.last_output_length = len(output)

    def finish(self) -> None:
        """
        Finish the progress bar and print a new line.
        """
        self.update(self.current)
        sys.stdout.write("\n")
        sys.stdout.flush()

    def _format_time(self, seconds:float) -> str:
        """
        Format time in seconds to a human-readable string.
        """
        if seconds < 0.1:
            return f"{seconds*1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            seconds = seconds % 60
            return f"{minutes}m {seconds:.0f}s"
        else:
            hours = int(seconds // 3600)
            seconds = seconds % 3600
            minutes = int(seconds // 60)
            seconds = seconds % 60
            return f"{hours}h {minutes}m {seconds:.0f}s"
