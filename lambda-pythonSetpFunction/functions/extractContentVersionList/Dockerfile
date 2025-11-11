FROM public.ecr.aws/lambda/python:3.13

#COPY app.py requirements.txt ./

COPY layers/common/python /opt/python
COPY functions/extractContentVersionList/app.py ./
COPY functions/extractContentVersionList/requirements.txt ./

RUN python3.13 -m pip install -r requirements.txt -t .


# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]

