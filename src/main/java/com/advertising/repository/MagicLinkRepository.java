package com.advertising.repository;

import com.advertising.model.entity.MagicLink;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface MagicLinkRepository extends JpaRepository<MagicLink, UUID> {

    Optional<MagicLink> findByToken(String token);

    void deleteByExpiresAtBefore(OffsetDateTime now);
}
