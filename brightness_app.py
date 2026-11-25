import asyncio
import io
import os
import queue
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from PIL import Image, ImageTk


class ImageProcessingPipeline:
    """
    Конвейер асинхронной многопоточной обработки изображений (Вариант 3: Повышение яркости).
    """

    def __init__(self, pool_size: int, brightness_delta: int, on_progress, on_log, on_complete):
        self.pool_size = pool_size
        self.brightness_delta = brightness_delta
        self.on_progress = on_progress
        self.on_log = on_log
        self.on_complete = on_complete

        self.cancel_event = threading.Event()
        self.read_done_event = threading.Event()
        self.queue_condition = threading.Condition()
        self.active_semaphore = threading.Semaphore(pool_size)

        self.critical_lock = threading.Lock()
        self.task_queue = queue.Queue()
        self.result_strips = {}
        self.processed_fragments = 0
        self.total_fragments = 0
        self.shared_log = []
        self.workers = []

    def log(self, message: str):
        with self.critical_lock:
            entry = f"[{time.strftime('%H:%M:%S')}] {message}"
            self.shared_log.append(entry)
        self.on_log(entry)

    async def _async_read_file(self, filepath: str) -> bytes:
        self.log(f"Асинхронный читатель: старт чтения '{os.path.basename(filepath)}'")
        buffer = io.BytesIO()
        with open(filepath, "rb") as f:
            while not self.cancel_event.is_set():
                chunk = await asyncio.to_thread(f.read, 65536)
                if not chunk:
                    break
                buffer.write(chunk)
        if self.cancel_event.is_set():
            raise asyncio.CancelledError("Чтение отменено пользователем.")
        self.log(f"Асинхронный читатель: прочитано {buffer.tell()} байт.")
        return buffer.getvalue()

    async def _async_write_file(self, filepath: str, image: Image.Image):
        if self.cancel_event.is_set():
            self.log("Асинхронный писатель: запись отменена.")
            return
        self.log(f"Асинхронный писатель: сохранение в '{os.path.basename(filepath)}'...")
        out_buffer = io.BytesIO()
        ext = os.path.splitext(filepath)[1].lower()
        img_format = "JPEG" if ext in (".jpg", ".jpeg") else "PNG"
        if img_format == "JPEG" and image.mode == "RGBA":
            image = image.convert("RGB")
        await asyncio.to_thread(image.save, out_buffer, format=img_format)
        data = out_buffer.getvalue()
        temp_path = filepath + ".tmp"
        try:
            with open(temp_path, "wb") as f:
                for offset in range(0, len(data), 65536):
                    if self.cancel_event.is_set():
                        raise asyncio.CancelledError("Запись прервана отменой.")
                    await asyncio.to_thread(f.write, data[offset:offset + 65536])
            os.replace(temp_path, filepath)
            self.log("Асинхронный писатель: файл успешно сохранён.")
        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise

    def _worker_loop(self, worker_id: int):
        while not self.cancel_event.is_set():
            with self.queue_condition:
                while self.task_queue.empty() and not self.cancel_event.is_set():
                    self.queue_condition.wait(timeout=0.1)
                if self.cancel_event.is_set():
                    break
                try:
                    task = self.task_queue.get_nowait()
                except queue.Empty:
                    continue
            if task is None:
                self.task_queue.task_done()
                break
            strip_idx, y_start, strip_img = task
            with self.active_semaphore:
                if self.cancel_event.is_set():
                    self.task_queue.task_done()
                    break
                processed_strip = self._adjust_brightness_strip(strip_img, self.brightness_delta)
                with self.critical_lock:
                    if not self.cancel_event.is_set():
                        self.result_strips[strip_idx] = (y_start, processed_strip)
                        self.processed_fragments += 1
                    current, total = self.processed_fragments, self.total_fragments
                self.on_progress(current, total)
                self.task_queue.task_done()

    @staticmethod
    def _adjust_brightness_strip(strip: Image.Image, delta: int) -> Image.Image:
        mode = strip.mode
        if mode not in ("RGB", "RGBA"):
            strip = strip.convert("RGB")
            mode = "RGB"
        width, height = strip.size
        src_pixels = strip.load()
        out_strip = Image.new(mode, (width, height))
        dst_pixels = out_strip.load()
        has_alpha = (mode == "RGBA")
        for y in range(height):
            for x in range(width):
                px = src_pixels[x, y]
                r = min(255, max(0, px[0] + delta))
                g = min(255, max(0, px[1] + delta))
                b = min(255, max(0, px[2] + delta))
                dst_pixels[x, y] = (r, g, b, px[3]) if has_alpha else (r, g, b)
        return out_strip

    def run(self, input_path: str, output_path: str):
        threading.Thread(target=self._orchestrate, args=(input_path, output_path), daemon=True).start()

    def _orchestrate(self, input_path: str, output_path: str):
        start_time = time.perf_counter()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            raw_bytes = loop.run_until_complete(self._async_read_file(input_path))
            self.read_done_event.set()
            source_image = Image.open(io.BytesIO(raw_bytes))
            source_image.load()
            width, height = source_image.size
            self.log(f"Изображение декодировано: {width}x{height} px, режим {source_image.mode}.")
            self.workers = []
            for i in range(self.pool_size):
                t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
                t.start()
                self.workers.append(t)
            self.log(f"Пул из {self.pool_size} потоков запущен.")
            strip_height = max(1, height // (self.pool_size * 4))
            strips_count = 0
            for idx, y in enumerate(range(0, height, strip_height)):
                if self.cancel_event.is_set():
                    break
                fragment = source_image.crop((0, y, width, min(y + strip_height, height)))
                with self.queue_condition:
                    self.task_queue.put((idx, y, fragment))
                    strips_count += 1
                    self.queue_condition.notify()
            with self.critical_lock:
                self.total_fragments = strips_count
            self.log(f"Планировщик поставил в очередь {strips_count} фрагментов.")
            with self.queue_condition:
                for _ in range(self.pool_size):
                    self.task_queue.put(None)
                self.queue_condition.notify_all()
            for t in self.workers:
                t.join()
            if self.cancel_event.is_set():
                self.log("Обработка отменена. Все потоки корректно завершены.")
                self.on_complete(False, None, 0.0, "Обработка отменена пользователем.")
                return
            self.log("Сборщик результатов: объединение фрагментов...")
            result_mode = "RGBA" if source_image.mode == "RGBA" else "RGB"
            final_image = Image.new(result_mode, (width, height))
            with self.critical_lock:
                for idx in sorted(self.result_strips.keys()):
                    y_pos, strip_img = self.result_strips[idx]
                    final_image.paste(strip_img, (0, y_pos))
            loop.run_until_complete(self._async_write_file(output_path, final_image))
            elapsed = time.perf_counter() - start_time
            self.log(f"Конвейер завершён за {elapsed:.3f} сек.")
            self.on_complete(True, final_image, elapsed, "Обработка успешно завершена!")
        except asyncio.CancelledError as ce:
            self.log(f"Операция прервана: {ce}")
            self.on_complete(False, None, 0.0, str(ce))
        except Exception as exc:
            self.log(f"Ошибка конвейера: {exc}")
            self.on_complete(False, None, 0.0, f"Ошибка: {exc}")
        finally:
            self.cancel_event.set()
            with self.queue_condition:
                self.queue_condition.notify_all()
            for t in self.workers:
                if t.is_alive():
                    t.join(timeout=1.0)
            loop.close()

    def cancel(self):
        self.log("Получен запрос на отмену обработки...")
        self.cancel_event.set()
        with self.queue_condition:
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                    self.task_queue.task_done()
                except queue.Empty:
                    break
            self.queue_condition.notify_all()


class BrightnessApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Вариант 3: Повышение яркости (Async + ThreadPool)")
        self.geometry("920x660")
        self.minsize(820, 580)
        self.input_path = None
        self.output_path = None
        self.pipeline = None
        self.preview_orig_tk = None
        self.preview_res_tk = None
        self._build_ui()

    def _build_ui(self):
        control_frame = ttk.LabelFrame(self, text="Параметры обработки", padding=10)
        control_frame.pack(fill=tk.X, padx=10, pady=5)
        self.btn_open = ttk.Button(control_frame, text="1. Выбрать изображение", command=self.select_input_file)
        self.btn_open.grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.lbl_file = ttk.Label(control_frame, text="Файл не выбран", width=35)
        self.lbl_file.grid(row=0, column=1, padx=5, pady=5, sticky="w")
        ttk.Label(control_frame, text="Яркость (Δ):").grid(row=0, column=2, padx=(15, 5), pady=5)
        self.brightness_var = tk.IntVar(value=50)
        self.scale_brightness = ttk.Scale(
            control_frame, from_=-150, to=200, variable=self.brightness_var,
            command=lambda v: self.lbl_brightness_val.config(text=f"{int(float(v)):+d}")
        )
        self.scale_brightness.grid(row=0, column=3, padx=5, pady=5)
        self.lbl_brightness_val = ttk.Label(control_frame, text="+50", width=5)
        self.lbl_brightness_val.grid(row=0, column=4, padx=5, pady=5)
        ttk.Label(control_frame, text="Потоков в пуле:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.pool_var = tk.IntVar(value=min(4, os.cpu_count() or 4))
        self.spin_pool = ttk.Spinbox(control_frame, from_=1, to=32, textvariable=self.pool_var, width=6)
        self.spin_pool.grid(row=1, column=1, padx=5, pady=5, sticky="w")
        btn_box = ttk.Frame(control_frame)
        btn_box.grid(row=1, column=2, columnspan=3, padx=10, pady=5, sticky="e")
        self.btn_start = ttk.Button(btn_box, text="2. Запустить обработку", command=self.start_processing, state=tk.DISABLED)
        self.btn_start.pack(side=tk.LEFT, padx=5)
        self.btn_cancel = ttk.Button(btn_box, text="Отмена", command=self.cancel_processing, state=tk.DISABLED)
        self.btn_cancel.pack(side=tk.LEFT, padx=5)

        progress_frame = ttk.Frame(self, padding=(10, 2))
        progress_frame.pack(fill=tk.X)
        self.progress_bar = ttk.Progressbar(progress_frame, mode="determinate")
        self.progress_bar.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        self.lbl_status = ttk.Label(progress_frame, text="Готов к работе", width=30)
        self.lbl_status.pack(side=tk.RIGHT)

        preview_frame = ttk.Frame(self, padding=10)
        preview_frame.pack(fill=tk.BOTH, expand=True)
        left_panel = ttk.LabelFrame(preview_frame, text="Исходное изображение")
        left_panel.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        self.canvas_orig = tk.Label(left_panel, text="Нет изображения", anchor="center")
        self.canvas_orig.pack(fill=tk.BOTH, expand=True)
        right_panel = ttk.LabelFrame(preview_frame, text="Результат (после обработки)")
        right_panel.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        self.canvas_res = tk.Label(right_panel, text="Ожидание обработки", anchor="center")
        self.canvas_res.pack(fill=tk.BOTH, expand=True)

        log_frame = ttk.LabelFrame(self, text="Журнал конвейера (защищён критической секцией)", padding=5)
        log_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        self.txt_log = tk.Text(log_frame, height=8, state=tk.DISABLED, font=("Consolas", 9))
        self.txt_log.pack(fill=tk.X)

    def select_input_file(self):
        path = filedialog.askopenfilename(
            title="Выберите изображение",
            filetypes=[("Изображения", "*.png *.jpg *.jpeg *.bmp *.tiff"), ("Все файлы", "*.*")]
        )
        if not path:
            return
        self.input_path = path
        self.lbl_file.config(text=os.path.basename(path))
        self.btn_start.config(state=tk.NORMAL)
        img = Image.open(path)
        self._show_preview(img, self.canvas_orig, is_orig=True)
        self.canvas_res.config(image="", text="Нажмите «Запустить обработку»")
        self._append_log(f"Выбран входной файл: {path}")

    def start_processing(self):
        if not self.input_path:
            return
        save_path = filedialog.asksaveasfilename(
            title="Куда сохранить результат?",
            initialfile="bright_" + os.path.basename(self.input_path),
            defaultextension=".png",
            filetypes=[("PNG", "*.png"), ("JPEG", "*.jpg"), ("BMP", "*.bmp")]
        )
        if not save_path:
            return
        self.output_path = save_path
        self.progress_bar["value"] = 0
        self.lbl_status.config(text="Выполняется...")
        self.btn_start.config(state=tk.DISABLED)
        self.btn_open.config(state=tk.DISABLED)
        self.btn_cancel.config(state=tk.NORMAL)
        self.pipeline = ImageProcessingPipeline(
            pool_size=int(self.pool_var.get()),
            brightness_delta=int(self.brightness_var.get()),
            on_progress=lambda c, t: self.after(0, self._update_progress_ui, c, t),
            on_log=lambda m: self.after(0, self._append_log, m),
            on_complete=lambda s, img, el, msg: self.after(0, self._finish_ui, s, img, el, msg)
        )
        self.pipeline.run(self.input_path, self.output_path)

    def cancel_processing(self):
        if self.pipeline:
            self.btn_cancel.config(state=tk.DISABLED)
            self.pipeline.cancel()

    def _update_progress_ui(self, current: int, total: int):
        if total > 0:
            percent = (current / total) * 100
            self.progress_bar["value"] = percent
            self.lbl_status.config(text=f"Фрагментов: {current} / {total} ({percent:.0f}%)")

    def _append_log(self, message: str):
        self.txt_log.config(state=tk.NORMAL)
        self.txt_log.insert(tk.END, message + "\n")
        self.txt_log.see(tk.END)
        self.txt_log.config(state=tk.DISABLED)

    def _finish_ui(self, success: bool, result_image: Image.Image, elapsed: float, status_msg: str):
        self.btn_start.config(state=tk.NORMAL)
        self.btn_open.config(state=tk.NORMAL)
        self.btn_cancel.config(state=tk.DISABLED)
        if success and result_image is not None:
            self.lbl_status.config(text=f"Готово за {elapsed:.2f} с")
            self._show_preview(result_image, self.canvas_res, is_orig=False)
            messagebox.showinfo("Готово", f"{status_msg}\nВремя выполнения: {elapsed:.3f} сек.")
        else:
            self.progress_bar["value"] = 0
            self.lbl_status.config(text="Отменено / Ошибка")
            self.canvas_res.config(image="", text=status_msg)

    def _show_preview(self, img: Image.Image, label_widget: tk.Label, is_orig: bool):
        preview = img.copy()
        preview.thumbnail((400, 320), Image.Resampling.LANCZOS)
        tk_img = ImageTk.PhotoImage(preview)
        if is_orig:
            self.preview_orig_tk = tk_img
        else:
            self.preview_res_tk = tk_img
        label_widget.config(image=tk_img, text="")


if __name__ == "__main__":
    app = BrightnessApp()
    app.mainloop()
