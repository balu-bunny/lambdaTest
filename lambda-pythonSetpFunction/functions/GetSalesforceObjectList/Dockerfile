FROM public.ecr.aws/lambda/python:3.13

COPY layers/common/python /opt/python
COPY functions/GetSalesforceObjectList/app.py ./
COPY functions/GetSalesforceObjectList/requirements.txt ./
RUN python3.13 -m pip install -r requirements.txt -t .


# Command can be overwritten by providing a different command in the template directly.
CMD ["app.lambda_handler"]
