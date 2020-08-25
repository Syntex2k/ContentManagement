# Anforderungen

### Szenario

- Der Nutzer hat den Indikator Smoking gewählt.
- Andere Indikatoren werden nur angedeutet (Ausarbeitung in die Breite).

#### Paragraph View

- Anzeige von n Smoking Paragraphen.
- Weitere n anzeigen (Pagination).
- In den Paragraphen sind die Smoking Sentences markiert. COLOR nach Sentiment.
- In den Pargraphen sind die Indikatoren markiert (eventuell). BOLD
- Markierungen sind als Button (Link) zum Sentence View verfügbar.

#### Sentence View

- Anzeige von Smoking Sentence.
- Indikatoren sind markiert.
- POS Tags sind markiert (nach Verben, Adjektiven und Substantiven).
- Sentiment wird dargestellt (Color / Score). Welche Stärke iner möglichen Beziehung zu Corona.
- Änhliche 5 Sentences werden angezeigt.
- Von den einzelnen Sentences komme ich zurück auf den Paragraphen, um den Kontext einzusehen.

***

### Piplines

#### Indicator Pipeline

- Selektiert Paragraphen nach Indicator (Indicator Dictionary). => Smoking Paragraph Table
- Extrahiert Sätze, in denen der Indikator vorkommt. => Smoking Sentence Table

#### POS Pipeline

- Untersucht Smoking Sentences und Tagged POS (Adjective, Verben, Nomen => Filter).
- Das Resultat muss in die Sentence Table.

#### Sentiment Pipeline (low priority)

- Versuch einer Qualitative Einschätzung.
- Gut | Schlecht bzw. Affirmativ | Verneinend (kausale Beziehung). 
- Gut | Schlecht => Adjektive mit positiver bzw. negativer Sentiment Score.
- Kausal | Non Kausal => Verben und Kunjunktionen  (causes, results, result).
- Baut sehr stark auf der POS Pipeline auf, wenn nur einzelne Worgruppen gefiltert werden.

#### Sentence Vektorizierer

- Stellt alle Smoking Sentences als Vektor dar.
- Vokabular ist der Wortschatz aller Smoking Sentences.
- Eventuell gescheite Reduktion vornehmen. z.B. POS Words.
- Ziel: Nähebeziehung zwischen Sätyen ermitteln, d.h. 5 ähnlichste.

#### Indicator Word Embedding

- Welche Worte kommen in der Regel im Kontext des Indikators vor.
- Cluster, häufigste Worte (hoffentlich Corona bezogen).
- Geschait qualitativ filtern. Worte ohne Information raus.

#### 

