from rastervision.pipeline.pipeline_config import PipelineConfig
from rastervision.pipeline.config import register_config


@register_config('culvert_vision.test_pipeline_config')
class TestPipelineConfig(PipelineConfig):
    message: str = 'hello'

    def build(self, tmp_dir):
        from rastervision.culvert_vision.test_pipeline import TestPipeline
        return TestPipeline(self, tmp_dir)
