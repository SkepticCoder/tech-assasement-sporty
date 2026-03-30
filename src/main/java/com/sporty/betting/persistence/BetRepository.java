package com.sporty.betting.persistence;

import com.sporty.betting.domain.model.Bet;
import com.sporty.betting.domain.model.BetStatus;
import java.util.List;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface BetRepository extends CrudRepository<Bet, String> {

  List<Bet> findByEventIdAndStatus(String eventId, BetStatus status);

  List<Bet> findByEventId(String eventId);
}
