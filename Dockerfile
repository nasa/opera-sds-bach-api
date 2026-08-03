FROM hysds/pge-base:develop

MAINTAINER OPERA PCM 

WORKDIR /home/ops
# Clone wrapper code
COPY . /home/ops/bach-api
# RUN cd /home/ops/bach-api
# RUN cd bach-api
ENV PYTHONPATH=/home/ops
# COPY . /home/ops/bach-api
RUN cd bach-api/accountability_api && sudo /home/ops/verdi/bin/pip install -e . \
 && sudo /home/ops/verdi/bin/pip install pandas
#  && bach-api/docker/run_tests.sh
# RUN ls
# RUN pwd && cd bach-api/accountability_api && sudo /home/ops/verdi/bin/pip install -e . \
#  && sudo /home/ops/verdi/bin/pip install pandas \
#  && ./docker/run_tests.sh



#WORKDIR /home/ops/accountability-ui-api/accountability_api
#CMD ["/home/ops/verdi/bin/python", "app.py"]

WORKDIR /home/ops/bach-api
CMD ["gunicorn", "--logger-class", "accountability_api.setup_loggers.GunicornLogger", "--access-logfile", "-", "--enable-stdio-inheritance", "--log-level", "INFO","--bind","0.0.0.0:8875","'accountability_api:create_app(\"accountability_api.settings.Config\")''"]
# gunicorn --logger-class accountability_api.setup_loggers.GunicornLogger --enable-stdio-inheritance --log-level INFO --bind 0.0.0.0:8875 'accountability_api:create_app("accountability_api.settings.Config")'
