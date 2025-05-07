

# Music Streaming Platform Analysis using PySpark

This project showcases how to analyze user behavior and trends on a fictional music streaming platform using Apache Spark Structured APIs. By processing listening logs and song metadata, we uncover actionable insights about genre preferences, song popularity, listener habits, and more.

---

##  Datasets

### 1. `songs_metadata.csv`
Describes the available songs in the catalog.

| Column     | Description                                 |
|------------|---------------------------------------------|
| `song_id`  | Unique ID for each song                     |
| `title`    | Title of the song                           |
| `artist`   | Artist name                                 |
| `genre`    | Genre category (e.g., Rock, Pop, Jazz)      |
| `mood`     | Song mood (e.g., Happy, Sad, Chill)         |

### 2. `listening_logs.csv`
Captures individual play events from users.

| Column        | Description                                  |
|---------------|----------------------------------------------|
| `user_id`     | Unique user identifier                       |
| `song_id`     | ID of the played song                        |
| `timestamp`   | Date and time of the listening event         |
| `duration_sec`| Time (in seconds) the user listened to song  |

---

## Project Setup

### Dependencies

Install the required packages:

```bash
pip install pyspark pandas faker
```

###  Generate Data

Run the data generator:

```bash
python data_generator.py
```

This will create:
- `data/songs_metadata.csv`
- `data/listening_logs.csv`

---

##  Run the Analysis Script

```bash
spark-submit music_streaming_analysis.py
```

All analytical results will be saved inside the `output/` directory.

---

##  Task-by-Task Breakdown

---

### 1. Find Each User’s Favorite Genre

**Purpose:**  
Understand each user's most preferred genre based on their listening history.

**How it works:**
- Count how many times each user listened to each genre.
- Use a window function (`row_number`) to rank genres by play count per user.
- Select the top-ranked genre for each user.

**Why it matters:**  
This reveals individual preferences and helps create personalized content.

📁 Output saved to: `output/user_favorite_genres/`

---

###  2. Calculate Average Listen Time per Song

**Purpose:**  
Measure average engagement with each song.

**How it works:**
- Group by `song_id`
- Use Spark’s `avg()` function on `duration_sec`

**Why it matters:**  
This identifies whether users are listening to full tracks or skipping early.

📁 Output saved to: `output/avg_listen_time_per_song/`

---

###  3. Top 10 Most Played Songs This Week

**Purpose:**  
Identify the most popular songs from the current week.

**How it works:**
- Filter plays from the past week (e.g., from "2025-03-17").
- Count how many times each song was played.
- Sort and select the top 10.

**Why it matters:**  
This mimics weekly charts and can support playlist creation and trend detection.

📁 Output saved to: `output/top_songs_this_week/`

---

### 4. Recommend “Happy” Songs to “Sad” Listeners

**Purpose:**  
Provide uplifting song suggestions to users who often listen to sad music.

**How it works:**
- Identify users with at least 2 "Sad" song plays.
- Cross join them with the catalog of "Happy" songs.
- Use a window function to recommend up to 3 unique Happy songs per user that they haven't heard.

**Why it matters:**  
Builds mood-aware recommendation systems and increases user retention.

📁 Output saved to: `output/happy_recommendations/`

---

### 5. Compute Genre Loyalty Score

**Purpose:**  
Measure how loyal users are to their favorite genre.

**How it works:**
- Count total songs played by each user.
- Count how many plays belong to their most played genre.
- Calculate: `loyalty_score = top_genre_count / total_plays`.
- Filter users with score > 0.8.

**Why it matters:**  
Identifies genre-committed users who might appreciate genre-specific features or offers.

📁 Output saved to:  
- `output/genre_loyalty_scores/` (if any loyal users exist)  
- `output/loyalty_message.csv` (if none pass threshold)

---

### 6. Identify Night Owl Users

**Purpose:**  
Find users who frequently listen to music during late-night hours (12 AM – 5 AM).

**How it works:**
- Extract the hour from `timestamp`.
- Filter users whose listening time falls between 00:00 and 05:00.
- Return distinct `user_id`s.

**Why it matters:**  
This insight helps tailor late-night content or personalized notifications.

📁 Output saved to: `output/night_owl_users/`

---

###  7. Save Enriched Listening Logs

**Purpose:**  
Merge listening logs with song metadata to create a rich dataset for all advanced queries.

**How it works:**
- Join `listening_logs` with `songs_metadata` using `song_id`.
- Save the full combined result.

**Why it matters:**  
This provides an integrated view of both user behavior and song features.

📁 Output saved to: `output/enriched_logs/`

---

