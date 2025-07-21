import io
import os
from dotenv import load_dotenv
import ftplib

load_dotenv("config.env")

FTP_HOST = os.getenv('FTP_HOST')
FTP_PORT = os.getenv('FTP_PORT')
FTP_USER = os.getenv('FTP_USER')
FTP_PASSWORD = os.getenv('FTP_PASSWORD')


def ftp_establish_connection():

    ftp = ftplib.FTP()
    ftp.connect(FTP_HOST, int(FTP_PORT))
    print(ftp.getwelcome())
    ftp.login(FTP_USER, FTP_PASSWORD)

    return ftp


class FtpHandler:

    def __init__(self):
        self.ftp = ftp_establish_connection()

    def create_dir(self, path):
        for dir in path.split("/"):
            if not self.exists(dir):
                self.ftp.mkd(dir)
            self.ftp.cwd(dir)
        self.ftp.cwd("/")

    def exists(self, path):
        try:
            self.ftp.cwd(path)
            self.ftp.cwd("..")
            return True
        except Exception:
            return False

    def send_to_ftp(self, file_path, dest_dir):
        filename = os.path.basename(file_path)
        print(f"Uploading '{filename}' to '{dest_dir}'")

        try:
            self.create_dir(dest_dir)
            self.ftp.cwd(dest_dir)

            with open(file_path, "rb") as file:
                self.ftp.storbinary(f"STOR {filename}", file)

            print(f"Upload complete: {filename}")

        except Exception as e:
            print(f"Error uploading '{filename}': {e}")

        finally:
            self.ftp.cwd("/")  # Always return to root
            self.close()

    def download_from_ftp(self, remote_path, local_path):
        with open(local_path, 'wb') as f:
            self.ftp.retrbinary(f"RETR {remote_path}", f.write)
        print(f"Downloaded '{remote_path}' to '{local_path}'")

    def list_files_in_dir(self, remote_dir, pattern=None):
        file_paths = []
        try:
            self.ftp.cwd(remote_dir)
            items = self.ftp.nlst()

            for item in items:
                try:
                    self.ftp.cwd(item)  # Check if it's a dir
                    self.ftp.cwd("..")  # It's a folder, skip it
                except ftplib.error_perm:
                    # It's a file
                    if pattern is None or item.lower().endswith(pattern.lower()):
                        full_path = f"{remote_dir.rstrip('/')}/{item}"
                        file_paths.append(full_path)

        except Exception as e:
            print(f"Error accessing {remote_dir}: {e}")

        return file_paths



    def close(self):
        self.ftp.close()