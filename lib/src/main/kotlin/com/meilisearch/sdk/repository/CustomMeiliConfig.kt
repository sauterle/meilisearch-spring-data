package com.meilisearch.sdk.repository
import com.meilisearch.sdk.Config
import com.meilisearch.sdk.HttpClient
import okhttp3.OkHttpClient
import java.lang.reflect.Field
import java.time.Duration


class CustomMeiliConfig(config: Config) {

    private val timeoutHttPClient = OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(300))
        .callTimeout(Duration.ofSeconds(300))
        .readTimeout(Duration.ofSeconds(300))
        .writeTimeout(Duration.ofSeconds(300))
        .build()

    init {
        val httpClientHolder: HttpClient = config.httpClient
        val declaredCustomHttpClient: Field = httpClientHolder.javaClass.getDeclaredField("client")
        declaredCustomHttpClient.isAccessible = true

        val customOkHttpClient: Any = declaredCustomHttpClient.get(httpClientHolder)
        val declaredOkHttpClientField = customOkHttpClient.javaClass.getDeclaredField("client")
        declaredOkHttpClientField.isAccessible = true
        declaredOkHttpClientField.set(customOkHttpClient, timeoutHttPClient)
    }

    val alteredConfig: Config = config

}

