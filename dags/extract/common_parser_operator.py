import requests
import logging


def random_api_download(api_url: str, retry_num_max: int, **context) -> str:
    with requests.Session() as s:
        s.stream = True

        retry_num = 1
        try:
            while retry_num <= retry_num_max:
                response = s.get(api_url)
                if response.status_code != 200:
                    raise Exception('API problem')

                logging.info(f"File_name: data_{context['data_interval_end']}")
                with open(f"data_{context['data_interval_end']}.json", 'wb') as fp:
                    chunk_iter = response.iter_content(2048, decode_unicode=False)
                    for chunk in chunk_iter:
                        fp.write(chunk)
                    break
        except Exception as e:
            retry_num += 1
            logging.info(f"ERROR: {e}")

        if retry_num > retry_num_max:
            raise Exception('Problem with writing')

    return f"data_{context['data_interval_end']}.json"
