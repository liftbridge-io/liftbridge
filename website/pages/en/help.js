const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

function Help(props) {
  const {config: siteConfig, language = ''} = props;
  const {baseUrl, docsUrl} = siteConfig;
  const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
  const docUrl = doc => `${baseUrl}${docsPart}${doc}`;

  const supportLinks = [
    {
      content: `Learn more using the [documentation on this site.](${docUrl(
        'overview.html',
      )})`,
      title: 'Browse Docs',
    },
    {
      content: 'You can follow and contact us on <a href="https://twitter.com/liftbridge_io">Twitter</a>.',
      title: 'Twitter',
    },
    {
      content: 'Visit the <a href="https://github.com/liftbridge-io/liftbridge">GitHub</a> repo to browse ' +
               'and submit issues. Create pull requests to contribute bug fixes and new features.',
      title: 'GitHub',
    },
  ];

  return (
    <div className="docMainWrapper wrapper">
      <Container className="mainContainer documentContainer postContainer">
        <div className="post">
          <header className="postHeader">
            <h1>Need help?</h1>
          </header>
          <p>If you need help with Liftbridge, try one of the support channels below.</p>
          <GridBlock contents={supportLinks} layout="threeColumn" />
        </div>
      </Container>
    </div>
  );
}

module.exports = Help;
