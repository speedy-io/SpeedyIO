import os
import json
import logging
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import argparse
from datetime import datetime
from tqdm import tqdm
import humanfriendly
import scipy
import numpy as np

# Global configuration dictionary
CONFIG = {
    # Specify properties to plot for each log type
    "properties_to_plot": {
        "disk_io_usage": ["wMB/s", "rMB/s", "r/s", "w/s", "aqu-sz", "rareq-sz", "wareq-sz", "r_await", "w_await"],
        "disk_usage": ["size", "used", "avail", "pcent"],
        "memory_usage": ["total", "used", "free"],
        "network_usage": ["rxpck/s", "txpck/s", "rxkB/s", "txkB/s"]
    },
    # Control which identifiers are plotted for each log type
    "identifiers_to_plot": {
        "disk_io_usage": ["nvme0n1"],
        "disk_usage": ["/dev/sdc1"],
        "memory_usage": ["all"],
        "network_usage": ["enp1s0f0", "enp6s0f0", "lo"]
    },
    # Beautified names for file types
    "file_titles": {
        "disk_io_usage": "Disk I/O Usage",
        "disk_usage": "Disk Usage",
        "memory_usage": "Memory Usage",
        "network_usage": "Network Usage"
    }
}

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class LogFileProcessor:
    def __init__(self, filepath, properties_to_plot, identifiers_to_plot, file_title):
        self.filepath = filepath
        self.properties_to_plot = properties_to_plot
        self.identifiers_to_plot = identifiers_to_plot
        self.file_title = file_title
        self.timestamps = []
        self.data = {}
        self.class_name = self.__class__.__name__
        self.identifier = None

    def parse_value(self, val):
        if val in [None, ""]:
            return None
        try:
            return float(val)
        except ValueError:
            if isinstance(val, str):
                if val.endswith("%"):
                    return float(val.strip("%"))
                return humanfriendly.parse_size(val, binary=False)
            raise

    def load_data(self):
        logging.info(f'{self.class_name}: Loading data from {self.filepath}')
        with open(self.filepath, 'r') as file:
            lines = file.readlines()
            for line in tqdm(lines, desc=f'{self.class_name} Loading'):
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as e:
                    logging.error(f'Error decoding JSON: {line.strip()}')
                    continue  # Skip to the next line
                if entry.get('values') is None or len(entry['values']) == 0:
                    logging.warning(f"{self.class_name}: Empty values detected, skipping data point")
                    continue
                timestamp = datetime.strptime(entry['timestamp'], "%Y-%m-%dT%H:%M:%S")
                self.timestamps.append(timestamp)
                for value in entry['values']:
                    identifier_value = value.get(self.identifier) if self.identifier else 'all'
                    if identifier_value == "tmpfs":  # TODO: Don't know what tmpfs is, fix this later for disk_usage.json
                        continue
                    if identifier_value not in self.identifiers_to_plot:
                        continue
                    if identifier_value not in self.data:
                        self.data[identifier_value] = {prop: [] for prop in self.properties_to_plot}
                    for prop, val in value.items():
                        if prop in self.properties_to_plot:
                            parsed_val = self.parse_value(val)
                            if parsed_val is not None:
                                self.data[identifier_value][prop].append(parsed_val)
        logging.info(f'{self.class_name}: Finished loading data')

        # Validation check
        for identifier_value, metrics in self.data.items():
            for prop, values in metrics.items():
                if len(values) != len(self.timestamps):
                    raise ValueError(
                        f"Mismatch in number of timestamps and data points for identifier {identifier_value} and property {prop} in {self.class_name}. "
                        f"Timestamps: {len(self.timestamps)}, Values: {len(values)}")

    def plot_data(self, pdf):
        logging.info(f'{self.class_name}: Plotting data')

        for identifier_value, metrics in self.data.items():
            for prop, values in metrics.items():
                # Convert values to a numpy array
                original_values = np.array(values)

                # Calculate Z-scores
                z_scores = np.abs(scipy.stats.zscore(original_values))

                # Identify outliers (Z-score > some_value)  3 is generally considered a good value
                outlier_mask = z_scores > 2.0

                # Create the inclusion mask (inverted outlier mask)
                inclusion_mask = ~outlier_mask

                # Filtered data without outliers
                filtered_values = original_values[inclusion_mask]
                filtered_timestamps = np.array(self.timestamps)[inclusion_mask]

                # Calculate outliers statistics
                num_outliers = np.sum(outlier_mask)
                percentage_outliers = (num_outliers / len(original_values)) * 100

                # Create a new figure with two subplots side by side
                fig, axs = plt.subplots(1, 2, figsize=(14, 5))

                title_str = f'{self.file_title}: {prop} over Time for {identifier_value}'

                # Plot original data on the left
                axs[0].plot(self.timestamps, original_values, label=f"{identifier_value} - {prop}", marker='o', markersize=2, linewidth=0.8)
                axs[0].set_xlabel('Time')
                axs[0].set_ylabel(prop)
                axs[0].set_title(title_str)
                axs[0].legend()

                # Plot filtered data (outliers removed) on the right
                axs[1].plot(filtered_timestamps, filtered_values,
                            label=f"{identifier_value} - {prop} (outliers removed)", color='red', marker='o', markersize=2, linewidth=0.8)
                axs[1].set_xlabel('Time')
                axs[1].set_ylabel(prop)
                axs[1].set_title(
                    f'{title_str}\nRemoved {num_outliers} outliers ({percentage_outliers:.1f}% of the total data)'
                )
                axs[1].legend()

                # Save the figure to the PDF
                pdf.savefig(fig)
                plt.close(fig)

        logging.info(f'{self.class_name}: Finished plotting data')


class DiskIOUsageProcessor(LogFileProcessor):
    def __init__(self, filepath):
        super().__init__(filepath, CONFIG["properties_to_plot"]["disk_io_usage"],
                         CONFIG["identifiers_to_plot"]["disk_io_usage"], CONFIG["file_titles"]["disk_io_usage"])
        self.identifier = 'disk_device'


class DiskUsageProcessor(LogFileProcessor):
    def __init__(self, filepath):
        super().__init__(filepath, CONFIG["properties_to_plot"]["disk_usage"],
                         CONFIG["identifiers_to_plot"]["disk_usage"], CONFIG["file_titles"]["disk_usage"])
        self.identifier = 'source'


class MemoryUsageProcessor(LogFileProcessor):
    def __init__(self, filepath):
        super().__init__(filepath, CONFIG["properties_to_plot"]["memory_usage"],
                         CONFIG["identifiers_to_plot"]["memory_usage"], CONFIG["file_titles"]["memory_usage"])
        self.identifier = None


class NetworkUsageProcessor(LogFileProcessor):
    def __init__(self, filepath):
        super().__init__(filepath, CONFIG["properties_to_plot"]["network_usage"],
                         CONFIG["identifiers_to_plot"]["network_usage"], CONFIG["file_titles"]["network_usage"])
        self.identifier = 'interface'


def plot_system_monitoring_data(data_folder, output_folder, output_filename=f'system_monitoring_plots.pdf'):
    pdf_path = os.path.join(output_folder, output_filename)
    if os.path.exists(data_folder):
        os.makedirs(output_folder, exist_ok=True)
        with PdfPages(pdf_path) as pdf:
            plt.figure(figsize=(10, 10))
            plt.title(f"System usage plots for data in\n{data_folder}", fontdict={'size': 10, 'weight': 'bold'})
            plt.axis('off')
            pdf.savefig()  # saves the current figure into a pdf page
            plt.close()
            for log_file in os.listdir(data_folder):
                filepath = os.path.join(data_folder, log_file)
                if log_file == 'disk_io_usage.json':
                    processor = DiskIOUsageProcessor(filepath)
                elif log_file == 'disk_usage.json':
                    processor = DiskUsageProcessor(filepath)
                elif log_file == 'memory_usage.json':
                    processor = MemoryUsageProcessor(filepath)
                elif log_file == 'network_usage.json':
                    processor = NetworkUsageProcessor(filepath)
                else:
                    continue

                processor.load_data()
                processor.plot_data(pdf)
            logging.info(f"Finshed plotting system monitoring data for {output_filename}")
    else:
        logging.warning(f"plot_system_monitoring_data: data_folder:{data_folder} not found, skipping execution.")


if __name__ == "__main__":
    if __name__ == "__main__":
        # Set up argument parser
        parser = argparse.ArgumentParser(description="Plot system monitoring data.")

        # Define command line arguments
        parser.add_argument('--data_folder', type=str, required=True,
                            help='Path to the folder containing the monitoring data')
        parser.add_argument('--output_folder', type=str, required=True,
                            help='Path to the folder where the output plots will be saved')

        # Parse arguments
        args = parser.parse_args()

        # Call the function with the provided arguments
        plot_system_monitoring_data(args.data_folder, args.output_folder)
