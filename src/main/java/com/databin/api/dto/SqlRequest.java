package com.databin.api.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SqlRequest {
    private String sql;

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
}
