
1. Модули и два главных класса

• Импорты (строки 1–9):
	• asyncio, io — асинхронное чтение/запись байтов в буфер памяти.
	• threading, queue — потоки, потокобезопасная очередь и примитивы синхронизации.
	• tkinter, PIL (Image, ImageTk) — графическое окно и работа с пикселями.
• class ImageProcessingPipeline — вся логика многопоточной и асинхронной обработки (не знает ничего про кнопки и окна).
• class BrightnessApp(tk.Tk) — графический интерфейс (UI-поток).

2. Конструктор конвейера (ImageProcessingPipeline.__init__)

Здесь создаются общие ресурсы и объекты синхронизации:
• self.cancel_event = threading.Event() — флаг отмены (изначально сброшен; вызов .set() переводит его в True для всех потоков сразу).
• self.read_done_event = threading.Event() — флаг завершения чтения файла с диска.
• self.queue_condition = threading.Condition() — условная переменная для сна и пробуждения потоков при работе с очередью.
• self.active_semaphore = threading.Semaphore(pool_size) — семафор, ограничивающий число одновременно работающих обработчиков.
• self.critical_lock = threading.Lock() — мьютекс (критическая секция). Под его защитой (with self.critical_lock:) находятся:
	• self.result_strips = {} — словарь готовых полос {индекс_полосы: (y_координата, картинка)}.
	• self.processed_fragments и self.total_fragments — счётчики прогресса.
	• self.shared_log — общий список строк журнала.

3. Асинхронный ввод и вывод (_async_read_file и _async_write_file)

• _async_read_file:
	• Открывает исходный файл в бинарном режиме ("rb").
	• В цикле читает порциями по 65536 байт (64 КБ) через await asyncio.to_thread(f.read, 65536). to_thread выносит операцию с диском в фоновый системный поток, чтобы не блокировать цикл событий asyncio.
	• На каждой итерации проверяет if self.cancel_event.is_set(): — если нажата «Отмена», выбрасывает CancelledError и прекращает чтение.
• _async_write_file:
	• Кодирует собранную картинку в буфер памяти out_buffer (JPEG или PNG).
	• Пишет данные по 64 КБ сначала во временный файл filepath + ".tmp".
	• Если процесс не отменили, атомарно переименовывает его в итоговый файл (os.replace(temp_path, filepath)). Если была отмена или ошибка — блок except удаляет .tmp через os.remove(temp_path), чтобы не оставлять битый файл на диске
