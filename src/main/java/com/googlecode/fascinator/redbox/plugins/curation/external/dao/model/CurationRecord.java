package com.googlecode.fascinator.redbox.plugins.curation.external.dao.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
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
@Table(name = "curation_record")
public class CurationRecord implements Serializable{

	private static final long serialVersionUID = 1L;
	
	private String oid;
	
	private CurationJob curationJob;

	@Id
	@Column
	public String getOid() {
		return oid;
	}


	public void setOid(String oid) {
		this.oid = oid;
	}


	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "id", nullable = false)
	public CurationJob getCurationJob() {
		return curationJob;
	}


	public void setCurationJob(CurationJob curationJob) {
		this.curationJob = curationJob;
	}

	
}
