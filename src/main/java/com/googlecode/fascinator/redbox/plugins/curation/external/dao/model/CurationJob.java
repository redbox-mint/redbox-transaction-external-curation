package com.googlecode.fascinator.redbox.plugins.curation.external.dao.model;

import java.io.Serializable;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

/*
 * The Fascinator - Sequence
 * Copyright (C) 2008-2010 University of Southern Queensland
 * Copyright (C) 2012 Queensland Cyber Infrastructure Foundation (http://www.qcif.edu.au/)
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
@Entity
@Table(name = "curation_job")
public class CurationJob implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private int id;
	
	private String curationJobId;

	private String status = "INPROGRESS";
	
	private Set<CurationRecord> curationRecords;
	
	@Id
	@Column
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}
	
	@Column(name="external_curation_job_id")
	public String getCurationJobId() {
		return curationJobId;
	}

	public void setCurationJobId(String curationJobId) {
		this.curationJobId = curationJobId;
	}

	@OneToMany(fetch = FetchType.LAZY, mappedBy = "oid")
	public Set<CurationRecord> getCurationRecords() {
		return this.curationRecords;
	}

	public void setCurationRecords(Set<CurationRecord> curationRecords) {
		this.curationRecords = curationRecords;
	}

	public String getStatus() {
		return status;
	}
	
	@Column
	public void setStatus(String status) {
		this.status = status;
	}	
	
}
