package com.meilisearch.sdk.repository

import org.springframework.data.repository.CrudRepository

interface SearchDbRepository<T, ID>: CrudRepository<T, ID> {
}