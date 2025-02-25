import os

def collect_all_files(root_dir, output_file):
    with open(output_file, 'w', encoding='utf-8') as out_f:
        for dirpath, _, filenames in os.walk(root_dir):
            if "__pycache__" in dirpath:
                continue  # Skip __pycache__ directories
            for filename in filenames:
                if filename == output_file:
                    continue
                file_path = os.path.join(dirpath, filename)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                        content = file.read()
                        out_f.write(f"File: {file_path}\n")
                        out_f.write("=" * 80 + "\n")
                        out_f.write(content + "\n\n")
                        out_f.write("#" * 80 + "\n\n")
                except Exception as e:
                    print(f"Could not read {file_path}: {e}")

if __name__ == "__main__":
    root_directory = input("Enter the directory path to scan: ")
    output_filename = "collected_files.txt"
    collect_all_files(root_directory, output_filename)
    print(f"All files collected and saved in {output_filename}")