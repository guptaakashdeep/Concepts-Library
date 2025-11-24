import concurrent.futures
import hashlib
import queue
import sys
import threading
from contextlib import ExitStack
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as fs


class BackfeedPartFileMerger:
    """Merges part files into S3"""

    def __init__(
        self,
        s3_csv_path: str,
        local_output_path: str,
        header_file_path: str,
        gzip: bool = True,
    ):
        self.s3_csv_path = self._get_fspath(s3_csv_path)
        self.local_output_path = self._get_fspath(local_output_path, gzip)
        self.is_s3_uri = urlparse(local_output_path).scheme == "s3"
        self.max_downloader_threads = 8  # Number of threads to read the files in chunks
        self.chunk_size_bytes = 128 * 1024 * 1024  # 128 MB chunks
        self.max_chunks_in_queue = 16  # Max number of chunks to buffer in memory
        self.s3 = fs.S3FileSystem()
        self.gzip = gzip
        self.header_file_path = self._get_fspath(header_file_path)
        self._md5sum = hashlib.md5()

    @property
    def md5sum(self):
        """Gets the md5 sum of the merged file."""
        return self._md5sum.hexdigest()

    def _get_fspath(self, path, gzip=False):
        """Checks URI and updated the path with gz, if gzip true."""
        path = f"{path}.gz" if gzip and not path.endswith(".gz") else path
        parsed = urlparse(path)
        if parsed.scheme != "s3":
            return path
        return f"{parsed.netloc}/{parsed.path.lstrip('/')}"

    def _download_file_in_chunks(self, s3_file_path, s3_fs, chunk_queue):
        """Downloads a single file in chunks and puts them into a queue.
        Producer Function"""
        try:
            print(f"[Downloader] Starting: {s3_file_path}")
            with s3_fs.open_input_stream(s3_file_path) as s3_stream:
                # Buffer for incomplete line at the end of previous chunk
                leftover = b""
                while True:
                    chunk = s3_stream.read(self.chunk_size_bytes)
                    if not chunk:
                        # If there is data in leftover buffer, it's a full line
                        if leftover:
                            chunk_queue.put(leftover)
                        break

                    # Prepend leftover from last chunk
                    chunk = leftover + chunk
                    # Find the last line (complete record) in the chunk
                    last_newline = chunk.rfind(b"\n")
                    if last_newline != -1:
                        # Split chunk: complete records upto last '\n',
                        complete = chunk[: last_newline + 1]
                        leftover = chunk[last_newline + 1 :]
                        # If the queue is full, this call will be blocking
                        # automatically preventing memory from growing too large.
                        chunk_queue.put(complete)
                    else:
                        leftover = chunk
            # Handling last line if there are no `\n` character at the end of file
            if leftover:
                chunk_queue.put(leftover)
            print(f"[Downloader] Finished: {s3_file_path}")
        except Exception as e:
            print(f"Error in downloader for {s3_file_path}")
            raise e

    def _write_chunks_to_file(self, local_path, chunk_queue, num_files):
        """Takes chunks from a queue,  updates md5sum chunk by chunk, compress chunk if gzip is true,
        and writes them to a local file"""
        files_processed = 0
        with ExitStack() as stack:
            # Open base file stream
            base_file = stack.enter_context(open(local_path, "wb"))
            if self.gzip:
                out_stream = stack.enter_context(
                    pa.CompressedOutputStream(base_file, "gzip")
                )
            else:
                out_stream = base_file

            # Write chunks
            while files_processed < num_files:
                try:
                    # Get a chunk from the queue
                    chunk = chunk_queue.get()
                    if chunk is None:
                        files_processed += 1
                        continue
                    out_stream.write(chunk)
                    self._md5sum.update(chunk)
                except Exception as e:
                    print(f"Error in writer: {e}")
                    raise e
        compression_info = " (gzipped)" if self.gzip else ""
        print(f"[Writer] All files have been written{compression_info}.")

    def _write_chunks_to_s3_file(self, s3_path, chunk_queue, num_files):
        """Takes chunks from a queue and writes them to a S3 file. Updates md5sum chunk by chunk.
        Doesn't require gzipping chunks. Does it automatically based on filename
        extension"""
        files_processed = 0
        with self.s3.open_output_stream(s3_path) as out_stream:
            while files_processed < num_files:
                try:
                    # Get a chunk from the queue
                    chunk = chunk_queue.get()
                    if chunk is None:
                        files_processed += 1
                        continue
                    out_stream.write(chunk)
                    self._md5sum.update(chunk)
                except Exception as e:
                    print(f"Error in writer: {e}")
                    raise e
        print("[Writer] All files have been written.")

    def merge_part_files(self):
        try:
            file_selector = fs.FileSelector(self.s3_csv_path, recursive=True)
            source_files = [
                f.path for f in self.s3.get_file_info(file_selector) if f.is_file
            ]
            if not source_files:
                sys.exit(1)

            # A thread-safe queue with a max size to buffer chunks
            chunk_queue = queue.Queue(maxsize=self.max_chunks_in_queue)

            # Start single writer thread (consumer)
            print("Is output file in S3: ", self.is_s3_uri)
            if self.is_s3_uri:
                writer_thread = threading.Thread(
                    target=self._write_chunks_to_s3_file,
                    args=(self.local_output_path, chunk_queue, len(source_files)),
                )
            else:
                writer_thread = threading.Thread(
                    target=self._write_chunks_to_file,
                    args=(self.local_output_path, chunk_queue, len(source_files)),
                )
            writer_thread.start()

            # Write headers first
            self._download_file_in_chunks(self.header_file_path, self.s3, chunk_queue)

            # ThreadPoolExecutor for downloaders (producers)
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_downloader_threads
            ) as executors:
                futures = [
                    executors.submit(
                        self._download_file_in_chunks, path, self.s3, chunk_queue
                    )
                    for path in source_files
                ]
                # After each file is fully downloaded, put a sentinel value in the queue
                # to signal its completion to the writer.
                for future in concurrent.futures.as_completed(futures):
                    chunk_queue.put(None)

            # Wait for writer thread to finish writing all the chunks
            writer_thread.join()
            print(f"Successfully merged all the files into: {self.local_output_path}")
            # print(f"Calculated md5sum is: {self._md5sum.hexdigest()}")
        except Exception as e:
            raise e


# How to use this class for merging part-files
if __name__ == "__main__":
    headers_file = "s3://bucket/backfeed/headers/headers_facilities_feed.txt"
    s3_csv_path = "s3://bucket/backfeed/part-files/feed_20251105/"

    # Local path
    local_ouput_path = "/mnt/test/merged_exp.txt"

    # S3 Path
    s3_path = "s3://bucket-landing-zone/data-out/test/merged.csv.gz"

    # Writing merged file into local
    # merger = BackfeedPartFileMerger(s3_csv_path, local_ouput_path, headers_file)

    # Writing merged file into S3
    merger = BackfeedPartFileMerger(s3_csv_path, s3_path, headers_file)
    try:
        merger.merge_part_files()
        md5 = merger.md5sum
        print(f"md5sum: {md5}")
    except Exception as e:
        raise e
