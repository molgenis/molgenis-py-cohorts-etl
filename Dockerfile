FROM python:3

RUN pip install --upgrade pip

RUN adduser worker
USER worker
WORKDIR /home/worker

COPY --chown=worker:worker requirements.txt requirements.txt
RUN pip install --user -r requirements.txt

ENV PATH="/home/worker/.local/bin:${PATH}"

COPY --chown=worker:worker . .

CMD [ "python", "./run.py" ]