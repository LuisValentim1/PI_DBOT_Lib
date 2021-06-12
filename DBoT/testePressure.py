import logging
import threading
import time
import requests

ENDPOINT_INSERT = 'https://10.0.12.65/insert_into_db/'
tokens = ['']


def thread_function(name):
    logging.info("Thread %s: starting", name, token, sensorId)
    i=0
    times = []
    while(i<200):
        start = time.time()
        data = {"temperature": random.uniform(-10.0, 40.0), "humidity:": random.uniform(0.0, 100.0), "pressure": random.uniform(0.987, 1.0)}
        #jsonFile = json.dumps(data)
        request.post(ENDPOINT_INSERT+token+str(sensorId))
        finish = time.time()
        times.append(finish-start)
        logging.info("Thread %s: published", name)
        i = i+1
    plt.plot([range(200)], times, 'ro')
    plt.axis([0, 200, 0, max(times)])
    plt.show()
    logging.info("Thread %s: finishing", name)

if __name__ == "__main__":
    format = "%(asctime)s: %(message)s"
    logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%H:%M:%S")

    threads = list()
    for index in range(5):
        logging.info("Main    : create and start thread %d.", index)
        x = threading.Thread(target=thread_function, args=(index,tokens[index],index))
        threads.append(x)
        x.start()

    for index, thread in enumerate(threads):
        logging.info("Main    : before joining thread %d.", index)
        thread.join()
        logging.info("Main    : thread %d done", index)