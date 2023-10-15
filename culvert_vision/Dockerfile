FROM quay.io/azavea/raster-vision:pytorch-v0.21.2

# Uncomment if you add any non-RV requirements.
# COPY ./rastervision_culvert_vision/requirements.txt /opt/src/requirements.txt
# RUN pip install $(grep -ivE "rastervision_*" requirements.txt)

COPY ./rastervision_culvert_vision/ /opt/src/rastervision_culvert_vision/

ENV PYTHONPATH=/opt/src/rastervision_culvert_vision/:$PYTHONPATH

CMD ["bash"]
