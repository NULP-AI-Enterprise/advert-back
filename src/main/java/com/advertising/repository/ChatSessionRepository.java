package com.advertising.repository;

import com.advertising.model.entity.ChatSession;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ChatSessionRepository extends JpaRepository<ChatSession, UUID> {

    List<ChatSession> findByUserIdOrderByUpdatedAtDesc(String userId);

    @Query("SELECT s FROM ChatSession s LEFT JOIN FETCH s.messages WHERE s.id = :id")
    Optional<ChatSession> findByIdWithMessages(@Param("id") UUID id);
}
