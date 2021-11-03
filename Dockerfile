FROM python:3.8

COPY ./ /workdir

WORKDIR /workdir

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

RUN apt-get install apt-transport-https ca-certificates gnupg curl -y

RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

RUN apt-get update && apt-get install google-cloud-sdk -y

RUN gcloud auth activate-service-account --key-file ./google_credentials/serviceaccount.json --project=neochallenge

RUN pip install jupyter==1.0.0

ENTRYPOINT ["./entryfile.sh"]


