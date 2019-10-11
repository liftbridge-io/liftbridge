const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

function Acknowledgements(props) {
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
            <h1>Acknowledgements</h1>
          </header>
          <ul>
            <li>
              <a href="https://twitter.com/derekcollison">Derek Collison</a> and NATS team for building NATS and NATS Streaming and providing lots of inspiration.
            </li>
            <li>
              <a href="https://twitter.com/travisjeffery">Travis Jeffery</a> for building <a href="https://github.com/travisjeffery/jocko">Jocko</a>, a Go implementation
              of Kafka. The Liftbridge log implementation builds heavily upon the commit log
              from Jocko.
            </li>
            <li>
              <a href="http://kafka.apache.org">Apache Kafka</a> for inspiring large parts of the design, particularly around replication.
            </li>
          </ul>
        </div>
      </Container>
    </div>
  );
}

module.exports = Acknowledgements;
