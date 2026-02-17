package pdf

import (
	"github.com/johnfercher/maroto/v2"
	"github.com/johnfercher/maroto/v2/pkg/components/row"
	"github.com/johnfercher/maroto/v2/pkg/components/text"
	"github.com/johnfercher/maroto/v2/pkg/config"
	"github.com/johnfercher/maroto/v2/pkg/consts/align"
	"github.com/johnfercher/maroto/v2/pkg/consts/border"
	"github.com/johnfercher/maroto/v2/pkg/consts/fontstyle"
	"github.com/johnfercher/maroto/v2/pkg/consts/orientation"
	"github.com/johnfercher/maroto/v2/pkg/consts/pagesize"
	"github.com/johnfercher/maroto/v2/pkg/core"
	"github.com/johnfercher/maroto/v2/pkg/core/entity"
	"github.com/johnfercher/maroto/v2/pkg/props"
	"github.com/johnfercher/maroto/v2/pkg/repository"
)

type customConf struct {
	Colours struct {
		Black     *props.Cell
		LightGray *props.Cell
		DarkGray  *props.Cell
		White     *props.Cell
	}
	Widths  []int
	Headers []string
}

type Config struct {
	pdfConf    *entity.Config
	customConf customConf
}

type Handler struct {
	m core.Maroto

	cfg customConf
}

// конфиг должен храниться в сервисе
func GetConfig(headers []string) (*Config, error) {
	rf := repository.New()
	rf.AddUTF8Font("notosans", fontstyle.Normal, "./internal/fonts/NotoSans-Regular.ttf")
	fs, err := rf.Load()
	if err != nil {
		return nil, err
	}
	cfg := config.NewBuilder().
		WithOrientation(orientation.Horizontal).
		WithLeftMargin(10).
		WithTopMargin(15).
		WithRightMargin(10).
		WithPageNumber().
		WithPageSize(pagesize.A2).
		WithCustomFonts(fs).
		WithMaxGridSize(100).
		Build()

	whiteCell := &props.Cell{BackgroundColor: &props.WhiteColor, BorderType: border.Left | border.Right}
	lightGrayCell := &props.Cell{BackgroundColor: &props.Color{Red: 230, Green: 230, Blue: 230}, BorderType: border.Left | border.Right}
	darkGrayCell := &props.Cell{BackgroundColor: &props.Color{Red: 200, Green: 200, Blue: 200}, BorderType: border.Left | border.Right}
	blackCell := &props.Cell{BackgroundColor: &props.BlackColor, BorderType: border.Left | border.Right}
	Widths := []int{2, 3, 5, 16, 12, 10, 6, 5, 5, 5, 15, 4, 4, 4, 4}
	return &Config{
		pdfConf: cfg,
		customConf: customConf{
			Colours: struct {
				Black     *props.Cell
				LightGray *props.Cell
				DarkGray  *props.Cell
				White     *props.Cell
			}{
				Black:     blackCell,
				LightGray: lightGrayCell,
				DarkGray:  darkGrayCell,
				White:     whiteCell,
			},
			Widths:  Widths,
			Headers: headers,
		},
	}, nil
}

func NewHandler(cfg *Config) *Handler {
	return &Handler{
		m:   maroto.New(cfg.pdfConf),
		cfg: cfg.customConf,
	}
}

func (h *Handler) AddTitleAndHeader(title string) {
	h.m.AddRow(
		10,
		text.NewCol(100, title, props.Text{
			Family: "notosans",
			Size:   17,
			Align:  align.Center,
			Top:    1.5,
			Style:  fontstyle.Normal,
			Color:  &props.WhiteColor,
		}).WithStyle(h.cfg.Colours.Black),
	)
	hs := make([]core.Col, 0, len(h.cfg.Headers))
	for i, header := range h.cfg.Headers {
		w := h.cfg.Widths[i]
		hs = append(hs, text.NewCol(w, header, props.Text{
			Family: "notosans",
			Size:   12,
			Style:  fontstyle.Normal,
			Align:  align.Center,
			Top:    2,
			Left:   1,
			Right:  1,
		}).WithStyle(h.cfg.Colours.White))
	}
	h.m.AddRows(row.New(10).Add(hs...))
}

func (h *Handler) AddDataRows(batch [][]string) {
	for i, content := range batch {
		cs := make([]core.Col, 0, len(content))
		var cell *props.Cell
		if i&1 == 0 {
			cell = h.cfg.Colours.DarkGray
		} else {
			cell = h.cfg.Colours.LightGray
		}
		for i, c := range content {
			w := h.cfg.Widths[i]
			cs = append(cs, text.NewCol(w, c, props.Text{
				Family: "notosans",
				Size:   12,
				Style:  fontstyle.Normal,
				Top:    2,
				Left:   1,
				Right:  1,
			}).WithStyle(cell))
		}
		h.m.AddRows(row.New(10).Add(cs...))
	}
}

func (h *Handler) Generate() (core.Document, error) {
	return h.m.Generate()
}
