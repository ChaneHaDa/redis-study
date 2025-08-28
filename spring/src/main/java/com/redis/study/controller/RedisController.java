package com.redis.study.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/api/redis")
@RequiredArgsConstructor
@Tag(name = "Redis Operations", description = "Redis 기본 연산 API")
public class RedisController {

    private final RedisTemplate<String, Object> redisTemplate;

    @PostMapping("/set")
    @Operation(summary = "키-값 설정", description = "Redis에 키와 값을 저장합니다.")
    public ResponseEntity<String> set(
            @Parameter(description = "저장할 키", required = true)
            @RequestParam String key,
            @Parameter(description = "저장할 값", required = true)
            @RequestParam String value,
            @Parameter(description = "만료 시간(초), 선택사항")
            @RequestParam(required = false) Long expireSeconds) {

        try {
            if (expireSeconds != null && expireSeconds > 0) {
                redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(expireSeconds));
                log.info("Set key '{}' with value '{}' and expiration {} seconds", key, value, expireSeconds);
            } else {
                redisTemplate.opsForValue().set(key, value);
                log.info("Set key '{}' with value '{}'", key, value);
            }
            return ResponseEntity.ok("값이 성공적으로 저장되었습니다.");
        } catch (Exception e) {
            log.error("Failed to set value for key '{}': {}", key, e.getMessage());
            return ResponseEntity.internalServerError().body("저장 중 오류가 발생했습니다.");
        }
    }

    @GetMapping("/get/{key}")
    @Operation(summary = "값 조회", description = "Redis에서 키에 해당하는 값을 조회합니다.")
    public ResponseEntity<Object> get(
            @Parameter(description = "조회할 키", required = true)
            @PathVariable String key) {

        try {
            Object value = redisTemplate.opsForValue().get(key);
            if (value != null) {
                log.info("Retrieved value for key '{}': {}", key, value);
                return ResponseEntity.ok(value);
            } else {
                log.info("Key '{}' not found", key);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Failed to get value for key '{}': {}", key, e.getMessage());
            return ResponseEntity.internalServerError().body("조회 중 오류가 발생했습니다.");
        }
    }

    @DeleteMapping("/delete/{key}")
    @Operation(summary = "키 삭제", description = "Redis에서 지정된 키를 삭제합니다.")
    public ResponseEntity<String> delete(
            @Parameter(description = "삭제할 키", required = true)
            @PathVariable String key) {

        try {
            Boolean deleted = redisTemplate.delete(key);
            if (Boolean.TRUE.equals(deleted)) {
                log.info("Successfully deleted key '{}'", key);
                return ResponseEntity.ok("키가 성공적으로 삭제되었습니다.");
            } else {
                log.info("Key '{}' not found for deletion", key);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Failed to delete key '{}': {}", key, e.getMessage());
            return ResponseEntity.internalServerError().body("삭제 중 오류가 발생했습니다.");
        }
    }

    @GetMapping("/exists/{key}")
    @Operation(summary = "키 존재 여부 확인", description = "Redis에서 키의 존재 여부를 확인합니다.")
    public ResponseEntity<Boolean> exists(
            @Parameter(description = "확인할 키", required = true)
            @PathVariable String key) {

        try {
            Boolean exists = redisTemplate.hasKey(key);
            log.info("Key '{}' exists: {}", key, exists);
            return ResponseEntity.ok(exists);
        } catch (Exception e) {
            log.error("Failed to check existence of key '{}': {}", key, e.getMessage());
            return ResponseEntity.internalServerError().body(false);
        }
    }

    @GetMapping("/keys")
    @Operation(summary = "모든 키 조회", description = "Redis에 저장된 모든 키를 조회합니다.")
    public ResponseEntity<Set<String>> getAllKeys() {
        try {
            Set<String> keys = redisTemplate.keys("*");
            log.info("Retrieved {} keys", keys.size());
            return ResponseEntity.ok(keys);
        } catch (Exception e) {
            log.error("Failed to retrieve keys: {}", e.getMessage());
            return ResponseEntity.internalServerError().body(Set.of());
        }
    }

    @PostMapping("/hash/set")
    @Operation(summary = "Hash 필드 설정", description = "Redis Hash에 필드를 설정합니다.")
    public ResponseEntity<String> hashSet(
            @Parameter(description = "Hash 키", required = true)
            @RequestParam String key,
            @Parameter(description = "필드 이름", required = true)
            @RequestParam String field,
            @Parameter(description = "필드 값", required = true)
            @RequestParam String value) {

        try {
            redisTemplate.opsForHash().put(key, field, value);
            log.info("Set hash field '{}' in key '{}' with value '{}'", field, key, value);
            return ResponseEntity.ok("Hash 필드가 성공적으로 설정되었습니다.");
        } catch (Exception e) {
            log.error("Failed to set hash field: {}", e.getMessage());
            return ResponseEntity.internalServerError().body("Hash 설정 중 오류가 발생했습니다.");
        }
    }

    @GetMapping("/hash/get/{key}")
    @Operation(summary = "Hash 조회", description = "Redis Hash의 모든 필드와 값을 조회합니다.")
    public ResponseEntity<Map<Object, Object>> hashGet(
            @Parameter(description = "Hash 키", required = true)
            @PathVariable String key) {

        try {
            Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
            if (!entries.isEmpty()) {
                log.info("Retrieved {} hash entries for key '{}'", entries.size(), key);
                return ResponseEntity.ok(entries);
            } else {
                log.info("Hash key '{}' not found or empty", key);
                return ResponseEntity.notFound().build();
            }
        } catch (Exception e) {
            log.error("Failed to get hash entries for key '{}': {}", key, e.getMessage());
            return ResponseEntity.internalServerError().body(Map.of());
        }
    }
}
