from servicex.app.transforms import LogLevel, create_kibana_link_parameters


class TestAddRequestIdFilter:
    """Tests for add_request_id_filter using a real Kibana dashboard URL."""

    EXAMPLE_URL = (
        "https://atlas-kibana.mwt2.org:5601/s/servicex/app/dashboards"
        "?auth_provider_hint=anonymous1#/view/bb682100-5558-11ed-afcf-d91dad577662#"
        "?embed=true"
        "&_g=(filters:!(),refreshInterval:(pause:!t,value:1000),"
        "time:(from:now-24h/h,to:now))"
        "&_a=(filters:!((query:(match_phrase:(instance:servicex-unit-test))))"
        ",index:'923eaa00-45b9-11ed-afcf-d91dad577662')"
    )

    def test_preserves_base_url(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert result.startswith(
            "https://atlas-kibana.mwt2.org:5601/s/servicex/app/dashboards"
        )

    def test_preserves_auth_query(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "?auth_provider_hint=anonymous1" in result

    def test_preserves_view_path(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "#/view/bb682100-5558-11ed-afcf-d91dad577662" in result

    def test_includes_embed_true(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "embed=true" in result

    def test_preserves_instance_in_query(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "servicex-unit-test" in result

    def test_includes_request_id_in_query(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "abc-123" in result

    def test_includes_log_level_in_query(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.error
        )
        assert "level%3AERROR" in result

    def test_includes_app_state_filter(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "_a=" in result
        assert "requestId" in result

    def test_preserves_time_range(self):
        result = create_kibana_link_parameters(
            self.EXAMPLE_URL, "abc-123", log_level=LogLevel.info
        )
        assert "now-24h/h" in result
        assert "to:now" in result
