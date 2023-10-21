package com.wubaibao.flinkjava.code.test;
import java.util.*;

//扑克牌类
class Card {
    private String suit;
    private int rank;

    public Card(String suit, int rank) {
        this.suit = suit;
        this.rank = rank;
    }

    public String getSuit() {
        return suit;
    }

    public int getRank() {
        return rank;
    }

    @Override
    public String toString() {
        return rank + suit;
    }
}

//玩家类
class Player {
    private String name;
    private List<Card> hand;
    private int score;

    public Player(String name) {
        this.name = name;
        this.hand = new ArrayList<>();
        this.score = 0;
    }

    public String getName() {
        return name;
    }

    public List<Card> getHand() {
        return hand;
    }

    public int getScore() {
        return score;
    }

    public void addToHand(Card card) {
        hand.add(card);
    }

    public void removeFromHand(Card card) {
        hand.remove(card);
    }

    public void increaseScore(int points) {
        score += points;
    }
}

//卡牌比较器
class CardComparator implements Comparator<Card> {
    private Map<String, Integer> suitRank = new HashMap<>();

    public CardComparator() {
        suitRank.put("K", 6); // 大王
        suitRank.put("Q", 5); // 小王
        suitRank.put("D", 4); // 黑桃
        suitRank.put("C", 3); // 红桃
        suitRank.put("B", 2); // 梅花
        suitRank.put("A", 1); // 方片
    }

    @Override
    public int compare(Card card1, Card card2) {
        if (card1.getRank() != card2.getRank()) {
            return card2.getRank() - card1.getRank();
        }
        return suitRank.get(card2.getSuit()) - suitRank.get(card1.getSuit());
    }
}


//扑克游戏类
public class PokerGame {
    private List<Card> deck;
    private List<Player> players;
    private CardComparator cardComparator;

    public PokerGame() {
        initializeDeck(); // 初始化牌组
        initializePlayers();// 初始化玩家
        initializePlayerHand(); //给玩家发牌

        cardComparator = new CardComparator();
    }


    private void initializeDeck() {
        //花色：A:方片、B:梅花、C:红桃、D:黑桃
        List<String> suits = Arrays.asList("A", "B", "C", "D");

        //牌面：1-13
        List<Integer> ranks = new ArrayList<>();
        for (int i = 1; i <= 13; i++) {
            ranks.add(i);
        }

        //准备一副扑克牌
        deck = new ArrayList<>();
        for (String suit : suits) {
            for (int rank : ranks) {
                deck.add(new Card(suit, rank));
            }
        }

        deck.add(new Card("K", 20)); // 大王
        deck.add(new Card("Q", 20)); // 小王

        //打散扑克,并去除两张底牌
        Collections.shuffle(deck);
        deck.remove(deck.size() - 1);
        deck.remove(deck.size() - 1);
    }

    private void initializePlayers() {
        players = new ArrayList<>();
        players.add(new Player("甲"));
        players.add(new Player("乙"));
        players.add(new Player("丙"));
        players.add(new Player("丁"));

        //打散玩家顺序
        Collections.shuffle(players);
    }

    private void initializePlayerHand() {
        for (int i = 0; i < deck.size(); i++) {
            players.get(i % 4).addToHand(deck.get(i));
        }
    }


    private void playRound(int round) {
        System.out.println("第 " + round + " 轮开始：");

        //存储当前轮次每个玩家出的牌
        Map<Player, Card> roundCards = new HashMap<>();

        //自定义的出牌策略：同组成员中出最大的牌，同组另外成员出最小的牌
        //players(0)和players(2)是一组，players(1)和players(3)是一组
        compareGroupCards(players.get(0), players.get(2), roundCards);
        compareGroupCards(players.get(1), players.get(3), roundCards);

        //确定本轮胜者
        Player roundWinnerPlayer = determineRoundWinner(roundCards);
        System.out.println("本轮胜者: " + roundWinnerPlayer.getName());

        //更新玩家分数并从各个玩家手牌中移除本轮出的牌
        updateScoresAndRemoveCards(roundWinnerPlayer, roundCards);
    }


    private void compareGroupCards(Player player1, Player player2, Map<Player, Card> roundCards) {
        //对palyer1和player2的手牌进行排序
        player1.getHand().sort(new CardComparator());
        player2.getHand().sort(new CardComparator());

        //获取palyer1和player2的手牌中最大和最小的一张牌
        Card maxCard1 = player1.getHand().get(0);
        Card maxCard2 = player2.getHand().get(0);
        Card minCard1 = player1.getHand().get(player1.getHand().size() - 1);
        Card minCard2 = player2.getHand().get(player2.getHand().size() - 1);

        //比较palyer1和player2的手牌最大的牌，决定如何出牌
        int compare = cardComparator.compare(maxCard1, maxCard2);
        if (compare < 0) {
            //palyer1的手牌最大，palyer1出最大的牌，palyer2出最小的牌
            roundCards.put(player1, maxCard1);
            roundCards.put(player2, minCard2);
        } else {
            //palyer2的手牌最大，palyer2出最大的牌，palyer1出最小的牌
            roundCards.put(player1, minCard1);
            roundCards.put(player2, maxCard2);
        }
    }

    private Player determineRoundWinner(Map<Player, Card> roundCards) {
        Player roundWinnerPlayer = null;
        Card roundMaxCard = null;

        for (Map.Entry<Player, Card> playerCardEntry : roundCards.entrySet()) {
            Card currentCard = playerCardEntry.getValue();
            //第一次循环，roundMaxCard为null，直接赋值,后续循环比较大小,找出最大的牌和对应的玩家
            if (roundWinnerPlayer == null || cardComparator.compare(currentCard, roundMaxCard) < 0) {
                roundWinnerPlayer = playerCardEntry.getKey();
                roundMaxCard = currentCard;
            }
        }

        return roundWinnerPlayer;
    }

    private void updateScoresAndRemoveCards(Player roundWinnerPlayer, Map<Player, Card> roundCards) {
        for (Map.Entry<Player, Card> playerCardEntry : roundCards.entrySet()) {
            Player currentPlayer = playerCardEntry.getKey();
            Card currentCard = playerCardEntry.getValue();
            int points = currentCard.getRank();
            //如果当前玩家是本轮胜者，则累加分数
            roundWinnerPlayer.increaseScore(points);
            //从玩家手牌中移除本轮出的牌
            currentPlayer.removeFromHand(currentCard);
        }
    }

    private void determineWinner() {
        Player team1Player1 = players.get(0);
        Player team1Player2 = players.get(2);
        Player team2Player1 = players.get(1);
        Player team2Player2 = players.get(3);

        int team1Score = team1Player1.getScore() + team1Player2.getScore();
        int team2Score = team2Player1.getScore() + team2Player2.getScore();

        System.out.println("游戏结束，各玩家得分：");
        for (Player player : players) {
            System.out.println(player.getName() + ": " + player.getScore());
        }

        if (team1Score > team2Score) {
            System.out.println(team1Player1.getName() + "+" + team1Player2.getName() + " 组获胜");
        } else if (team1Score < team2Score) {
            System.out.println(team2Player1.getName() + "+" + team2Player2.getName() + " 组获胜");
        } else {
            System.out.println("平局");
        }
    }

    public static void main(String[] args) {
        PokerGame game = new PokerGame();
        //打印初始牌组
        System.out.println("初始牌组：");
        for (Player player : game.players) {
            System.out.println(player.getName() + ": " + player.getHand());
        }

        //比赛开始，共13轮比赛
        for (int round = 0; round < 13; round++) {
            game.playRound(round + 1);
        }

        //比赛结束，确定胜者
        game.determineWinner();
    }
}