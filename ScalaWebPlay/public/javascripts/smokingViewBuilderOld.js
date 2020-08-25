const state = {
  section: "main"
};

const setSection = (section) => {
    state.section = section
};

const findSectionByUrlPath = () => {
    const pathParts = window.location.pathname.split("/");
    const lastPart = pathParts[pathParts.length -1];
    if (lastPart === "paragraphs") setSection("paragraphs");
    else if (lastPart === "sentences") setSection("sentences");
    else setSection("main")
};

const fetchSmokingSentences = async () => {
    const response = await fetch('http://localhost:9000/smoking/api/sentences');
    return response.json();
};

const fetchSmokingParagraphs = async () => {
    const response = await fetch('http://localhost:9000/smoking/api/paragraphs');
    return response.json();
};

const addSentencesToView = (sentences) => {
    sentences.forEach(sentenceData => createSentenceCard(sentenceData))
};

const createSentenceCard = (sentenceData) => {
    const sentenceTextBlock = document.createElement('p');
    sentenceTextBlock.innerText = sentenceData.sentence;
    const sentenceCard = document.createElement('blockquote');
    sentenceCard.appendChild(sentenceTextBlock);
    document.getElementById("article").appendChild(sentenceCard)
};

const addParagraphsToView = (pargraphs) => {
    pargraphs.forEach(pargraphData => createParagraphCard(pargraphData))
};

const createParagraphCard = (paragraphData) => {
    const paragraphTextBlock = document.createElement('p');
    paragraphTextBlock.innerText = paragraphData.text;
    const paragraphCard = document.createElement('blockquote');
    paragraphCard.appendChild(paragraphTextBlock);
    document.getElementById("article").appendChild(paragraphCard)
};

const buildSentencesView = () => {
    fetchSmokingSentences().then(sentencesJson => addSentencesToView(sentencesJson.sentence));
};

const buildParagraphsView = () => {
    fetchSmokingParagraphs().then(paragraphsJson => addParagraphsToView(paragraphsJson))
};

const buildView = () => {
    if (state.section === "sentences") buildSentencesView();
    else if (state.section === "paragraphs") buildParagraphsView();
};

findSectionByUrlPath();
buildView();