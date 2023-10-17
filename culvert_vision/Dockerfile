FROM quay.io/azavea/raster-vision:pytorch-v0.21.2

###########################################
# Create a non-root user that matches local
# From: https://code.visualstudio.com/remote/advancedcontainers/add-nonroot-user#_creating-a-nonroot-user
###########################################
ARG USERNAME
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    #
    # [Optional] Add sudo support. Omit if you don't need to install software after connecting.
    && apt-get update \
    && apt-get install -y sudo \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME

###########################################
# Continue with original commands
###########################################

# Uncomment if you add any non-RV requirements.
COPY ./rastervision_culvert_vision/requirements.txt /opt/src/requirements.txt
RUN pip install $(grep -ivE "rastervision_*" requirements.txt)

COPY ./rastervision_culvert_vision/ /opt/src/rastervision_culvert_vision/

ENV PYTHONPATH=/opt/src/rastervision_culvert_vision/:$PYTHONPATH

CMD ["bash"]
