import logging
import os

def create_logger(logger_name, log_file):
    """
    Hàm tạo một logger với khả năng ghi log vào file và in ra console.

    Args:
        logger_name (str): Tên của logger
        log_file (str): Đường dẫn tới file log

    Returns:
        logging.Logger: Đối tượng logger đã được cấu hình.
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  
    logger.handlers = []
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.propagate = False

    return logger


def setup_loggers(log_files):
    """
    Hàm tạo một dictionary chứa các logger từ danh sách file log.
    Key của dictionary sẽ là tên file log (không bao gồm đường dẫn và phần mở rộng)

    Args:
        log_files (list): Danh sách các đường dẫn file log (ví dụ: ['logs/login.log', 'logs/error.log'])

    Returns:
        dict: Dictionary chứa các logger với key là tên file log
              (ví dụ: {'login': <Logger>, 'error': <Logger>})
    """
    loggers = {}
    for log_file in log_files:
        # Lấy tên file từ đường dẫn và bỏ phần mở rộng .log
        logger_name = os.path.basename(log_file).replace('.log', '')
        loggers[logger_name] = create_logger(logger_name, log_file)
    return loggers


# Ví dụ sử dụng
if __name__ == "__main__":
    log_files = ['logs/login.log', 'logs/error.log', 'logs/crawl.log']
    loggers = setup_loggers(log_files)
    
    # Bây giờ có thể sử dụng theo tên file
    loggers['login'].info('Đăng nhập thành công')
    loggers['error'].error('Có lỗi xảy ra')
    loggers['crawl'].info('Bắt đầu crawl dữ liệu')

    print("Hoàn tất ghi log. Kiểm tra các file trong thư mục logs/")